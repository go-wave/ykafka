# ykafka

通用kafka读取类。封装了kafka的low api读取接口，自己控制offset

## 什么时候要使用这个包？

* 需要自己控制订阅kafka的offset
* 希望能通过修改配置来快速订阅kafka的topic

## 使用方式

```
kafka, err := ykafka.New(Cfg.Config)
if err != nil {
    panic(err)
}

kafka.RegisterWorker("topic", Handler)

err = kafka.Start()
if err != nil {
    panic(err)
}

<-kafka.SafeQuit([]syscall.Signal{syscall.SIGHUP})
```

# Requirements

Go > 1.4

# License

WAVE TEAM MIT license.
