# Flink的时间语义

## 1 处理时间（Processing Time）

执行处理操作的机器的系统时间。

## 2 事件时间（Event Time）

每个事件在对应的设备上发生的时间，也就是数据生成的时间。

> Flink 早期版本默认的时间语义是处理时间；从 1.12 版本开始，Flink 已经将事件时间作为了默认的时间语义。