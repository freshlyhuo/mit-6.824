# MapReduce记录

MapReduce作为分布式系统的一种实现方式，属于**批处理计算**，适合大量输入、输入可独立处理，结果需要汇总
如：词频统计、日志分析、倒排索引、大规模数据清洗
Map:局部计算
Reduce:全局聚合

实验修改以下三个文件

```text
MapReduce/
├── src/
    ├── mr/
        ├── coordinator.go
        ├── rpc.go
        └── worker.go
```

## coordinator.go

定义了相关的结构体，为实现任务分割，多worker并行，超时重新分配等功能
提供worker rpc调用的函数：一个分配任务和一个任务完成回报

## rpc.go

定义的worker和coordinator间rpc通信的结构体

## worker.go

完成具体的任务如map\reduce

## 工作流程（以词频统计为例）

1. coordinator根据任务需要初始化
2. workers和coordinator建立rpc连接，获取任务：
   - map: 读取相关的文件，将（k,v）存入中间文件
   - reduce:读取中间文件，汇总写入输出
3. coordinator会在分配任务时检查是否有已分配任务超时
4. worker在完成对应任务后会回报coordinator，coordinator收到回报后检查该阶段任务是否全部已完成、已完成进入下个阶段
