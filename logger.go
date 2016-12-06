package ykafka

import(
    "github.com/go-wave/ylogger"
)

var Logger *ylogger.YLogger

func init() {
    Logger = ylogger.DefaultYLogger
}
