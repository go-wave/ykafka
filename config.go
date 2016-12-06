package ykafka

import (
	"encoding/json"
)

// 配置文件对象
type Config struct {
	Brokers []string    `json:"broker"`
	Topics  []*TopicCfg `json:"topic"`
	Keeper  *RedisCfg   `json:"keeper"`
	Debug   bool        `json:"debug"`
}

// topic对象
type TopicCfg struct {
	Name       string          `json:"name"`
	From       string          `json:"from"`
	Mode       string          `json:"mode"` // 可以为single和set两个值，分别对应两种模式
	ModeConfig json.RawMessage `json:"modeConfig"`
}

// single的消费模式
type Single struct{}

// set的消费模式
type Set struct {
	Num    int `json:"setNum"` // 到setNum条消息为一组
	Ticker int `json:"ticker"` // 或者到ticker（秒）为一组
}

// redis配置
type RedisCfg struct {
	Dns           string `json:"dns"`
	PoolMaxIdle   int    `json:"poolMaxIdle"`
	PoolMaxActive int    `json:"poolMaxActive"`
	PreFix        string `json:"preFix"`
	Ticker        string `json:"ticker"`
}
