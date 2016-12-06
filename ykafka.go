package ykafka

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/Shopify/sarama"
)

type Manager struct {
	conf       *Config
	workers    map[string]Worker
	setWorkers map[string]SetWorker
	pconsumers []*pconsumer
}

// model为single的时候一次消费一条
type Worker func(topic string, partition int32, offset int64, value []byte) error

func DefaultWorker(topic string, partition int32, offset int64, value []byte) error {
	return nil
}

// model为multi的时候一次消费多条
type SetWorker func(topic string, msgs []*sarama.ConsumerMessage) error

func DefaultSetWorker(topic string, msgs []*sarama.ConsumerMessage) error {
	return nil
}

func New(conf Config) (*Manager, error) {
	InitRedis(conf.Keeper)
	return &Manager{conf: &conf, workers: make(map[string]Worker), setWorkers: make(map[string]SetWorker)}, nil
}

func (m *Manager) RegisterWorker(topic string, worker Worker) error {
	m.workers[topic] = worker
	return nil
}

func (m *Manager) RegisterSetWorker(topic string, worker SetWorker) error {
	m.setWorkers[topic] = worker
	return nil
}

func (m *Manager) worker(topic string) (Worker, error) {
	if worker, ok := m.workers[topic]; ok {
		return worker, nil
	}
	return DefaultWorker, nil
}

func (m *Manager) setWorker(topic string) (SetWorker, error) {
	if worker, ok := m.setWorkers[topic]; ok {
		return worker, nil
	}
	return DefaultSetWorker, nil
}

func (m *Manager) initOffset(t *TopicCfg, partition int32, keeper *keeper) (int64, error) {
	var offset int64
	var err error
	if t.From == "end" {
		offset = sarama.OffsetNewest
	} else if t.From == "start" {
		offset = sarama.OffsetOldest
	} else {
		offset, err = keeper.Get(t.Name, partition)
		if err != nil {
			return 0, err
		}
	}
	return offset, nil
}

func (m *Manager) quit() error {
	for _, pconsumer := range m.pconsumers {
		pconsumer.command <- 1
	}
	// TODO： 等待所有pconsumer都完成再进行
	return nil
}

func (m *Manager) SafeQuit(signs []syscall.Signal) <-chan int {
	out := make(chan int)
	c := make(chan os.Signal, 2)
	signal.Notify(
		c,
		syscall.SIGKILL,
		syscall.SIGHUP,
		syscall.SIGQUIT,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGUSR1,
	)

	go func() {
		for {
			s := <-c
			if s.String() == "interrupt" {
				out <- 1
			}
		}
	}()

	return out
}

func (m *Manager) Start() error {
	cfg := m.conf

	Logger.Debug("ykafka start manager", "start")
	kafkaConf := sarama.NewConfig()
	client, err := sarama.NewClient(cfg.Brokers, kafkaConf)
	if err != nil {
		return err
	}

	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		return err
	}

	for _, topicCfg := range cfg.Topics {
		partitions, err := consumer.Partitions(topicCfg.Name)
		if err != nil {
			return err
		}

		topic := topicCfg.Name

		for _, partition := range partitions {
			keeper, err := NewKeeper(topic, partition, cfg.Keeper.PreFix, cfg.Keeper.Ticker)
			if err != nil {
				return err
			}

			offset, err := m.initOffset(topicCfg, partition, keeper)
			if err != nil {
				return err
			}

			pconsumer := &pconsumer{keeper: keeper, topic: topic, partition: partition, topicCfg: topicCfg, manager: m, consumer: consumer}
			m.pconsumers = append(m.pconsumers, pconsumer)

			switch topicCfg.Mode {
			case "single":
			case "":
				go pconsumer.singleMode(topicCfg, partition, offset)
			case "set":
				go pconsumer.setMode(topicCfg, partition, offset)
			}
		}
	}

	Logger.Debug("ykafka start manager", "end")

	return nil
}
