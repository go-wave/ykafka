package ykafka

import (
	"encoding/json"
	"time"

	"github.com/Shopify/sarama"
)

// 一个partition一个这个结构
type pconsumer struct {
	keeper    *keeper
	topic     string
	partition int32
	topicCfg  *TopicCfg
	manager   *Manager
	consumer  sarama.Consumer
	command   chan int // 1 结束
}

// single
func (m *pconsumer) singleMode(t *TopicCfg, partition int32, offset int64) error {
	worker, err := m.manager.worker(t.Name)
	if err != nil {
		return err
	}

	p, err := m.consumer.ConsumePartition(t.Name, partition, offset)
	if err != nil {
		return err
	}

	for {
		select {
		case msg := <-p.Messages():
			if msg != nil {
				Logger.Debug("get a message", string(msg.Key), msg.Offset, string(msg.Value))
				go worker(t.Name, partition, msg.Offset, msg.Value)
			}
			m.keeper.Set(t.Name, partition, msg.Offset)
		}
	}
}

func (m *pconsumer) setMode(t *TopicCfg, partition int32, offset int64) error {
	worker, err := m.manager.setWorker(t.Name)
	if err != nil {
		return err
	}

	p, err := m.consumer.ConsumePartition(t.Name, partition, offset)
	if err != nil {
		return err
	}

	modecfg := new(Set)
	err = json.Unmarshal(t.ModeConfig, modecfg)
	if err != nil {
		return err
	}

	msgs := make([]*sarama.ConsumerMessage, 0, modecfg.Num)
	ticker := time.NewTicker(time.Duration(modecfg.Ticker) * time.Second)

	for {
		select {
		case <-ticker.C:
			err := worker(t.Name, msgs)
			if err != nil {
				Logger.Warning("work set msgs got error", err.Error())
			}
			msgs = make([]*sarama.ConsumerMessage, 0, modecfg.Num)
		case msg := <-p.Messages():
			if msg != nil {
				msgs = append(msgs, msg)
			}
			m.keeper.Set(t.Name, partition, msg.Offset)

			if len(msgs) > modecfg.Num {
				err := worker(t.Name, msgs)
				if err != nil {
					Logger.Warning("work set msgs got error", err.Error())
				}
				msgs = make([]*sarama.ConsumerMessage, 0, modecfg.Num)
			}
		}
	}
}
