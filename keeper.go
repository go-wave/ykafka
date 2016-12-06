package ykafka

import (
	"bytes"
	"strconv"
	"sync"
	"time"
)

type keeper struct {
	lock      sync.RWMutex
	ticker    *time.Ticker
	topic     string
	partition int32
	offset    int64
	prefix    string
}

func offsetkey(topic string, partition int32, prefix string) string {

	var buffer bytes.Buffer
	buffer.WriteString(prefix)
	buffer.WriteString(":")
	buffer.WriteString(topic)
	buffer.WriteString(":")
	buffer.WriteString(strconv.FormatInt(int64(partition), 10))

	return buffer.String()
}

func (k *keeper) Get(topic string, partition int32) (int64, error) {
	k.lock.Lock()
	defer k.lock.Unlock()
	return k.offset, nil
}

func (k *keeper) Set(topic string, partition int32, offset int64) error {
	k.lock.Lock()
	defer k.lock.Unlock()
	k.offset = offset
	return nil

}

func (k *keeper) Close() {
	k.lock.RLock()
	defer k.lock.RUnlock()

	conn := RedisPool.Get()
	defer conn.Close()

	_, err := conn.Do("SET", offsetkey(k.topic, k.partition, k.prefix), k.offset)
	if err != nil {
		Logger.Warning("keeper set offset error", err.Error(), k.offset)
	}
	k.ticker.Stop()
}

func NewKeeper(topic string, partition int32, prefix string, ticker string) (*keeper, error) {
	keeper := &keeper{topic: topic, partition: partition, prefix: prefix}
	duration, err := time.ParseDuration(ticker)
	if err != nil {
		return nil, err
	}
	keeper.ticker = time.NewTicker(duration)

	go func() {
		conn := RedisPool.Get()

		for {
			select {
			case _ = <-keeper.ticker.C:
				key := offsetkey(topic, partition, prefix)
				_, err := conn.Do("SET", key, keeper.offset)
				if err != nil {
					Logger.Warning("keeper set offset error", err.Error(), key, keeper.offset)
				}
			}
		}
	}()

	return keeper, nil
}
