# 版本相关

2019年1月阿里Blink开源

目前版本1.9

# Flink 中的核心概念和基础篇

## 简单介绍

Flink是一个分布式处理引擎，用于无界和有界流的有状态计算

提供了分布式，容错性和资源管理的核心功能

提供多层高抽象的API：

https://www.processon.com/diagraming/60d6fd14f346fb5e35b4b555

自下而上依次是状态流process，DStream和Dset，table，sql

提供具体领域的学习库：机器学习库和图计算库

## Flink特性

高吞吐，低时延，高性能流处理

有事件时间的窗口操作

有状态的严格一次性语义exactly-once

支持高灵活度的窗口，基于时间，count，session以及data-driven的窗口操作

​	全局窗口会指定同一key的数据进入同一个窗口，必须结合自定义触发器来使用，因为全局窗口没有进行数据聚合的结束点

​	自定义触发器

​		基于处理时间或者事件时间处理过一个元素之后, 注册一个定时器, 然后指定的时间执行.

```JAVA
Context和OnTimerContext所持有的TimerService对象拥有以下方法:
currentProcessingTime(): Long //返回当前处理时间
currentWatermark(): Long //返回当前watermark的时间戳
registerProcessingTimeTimer(timestamp: Long):Unit //会注册当前key的processing time的定时器。当processing time到达定时时间时，触发timer。
registerEventTimeTimer(timestamp: Long): Unit //会注册当前key的event time 定时器。当水位线大于等于定时器注册的时间时，触发定时器执行回调函数。
deleteProcessingTimeTimer(timestamp: Long): Unit //删除之前注册处理时间定时器。如果没有这个时间戳的定时器，则不执行。
deleteEventTimeTimer(timestamp: Long): Unit //删除之前注册的事件时间定时器，如果没有此时间戳的定时器，则不执行。
```


支持具有BackPressure功能的持续流模型

基于轻量级分布式快照（Snapshot）实现的容错

一个运行时同时支持batch on streaming 处理 和 streaming处理

Flink在jvm实现了自己的内存管理

支持迭代计算

支持程序自动优化：避免特定情况下shuffle，排序等昂贵操作，中间结果有必要进行缓存

## Flink vs Spark Streaming 

主要的是Flink是标准的实时处理引擎，基于事件驱动。而spark streaming 是微批次的

### 架构模型区别

SS的主要架构为 master，worker，driver，executor

Flink的是JM,TM和Slot

### 任务调度区别

SS连续生成微批次的数据，构建DAG（有向无环图）SS会依次创建DstreamGraph，JobGenerator，JobScheduler

Flink根据用户提交的代码生成StreamGraph，经过优化生成JobGraph，JG提交给JM，JM根据JG生成EG（ExecutionGraph），ExecutionGraph是Flink调度最核心的数据模型，JM根据EG对job进行调度

### 时间机制区别

SS只支持处理时间

Flink支持事件时间，处理时间和注入时间

#### Flink注入时间：

事件进入Flink的时间，即将每一个事件在**数据源算子**的处理时间作为事件时间的时间戳，并自动生成水位线，无法处理迟到数据和乱序事件

### 容错机制

对于SS任务，可以设置checkpoint，发生故障可以从上次checkpoint恢复，但是只能保证不丢数，可能会重复，不是严格一次性语义

## Flink的组件栈

https://www.processon.com/diagraming/60f0e20ce401fd4fe050cc41

## Flink集群规模

Flink支持多少节点的集群规模（基于Flink on Yarn）

基于生产环节的集群规模，节点和内存情况

Flink基础编程模型

```java
//source阶段
DataStream<String> lines =  env.addSource(new 
                                	FlinkKafkaConsumer<> (...));
//Transformation阶段
DataStream<String> events = lines.map((line) -> parse(line));
DataStream<Statistics> stats = events
		.keyby("id")
    	.timeWindow(Time.seconds(10))
    	.apply(new MyWindowAggFun());
//sink阶段
stats.addSink(new RollingSink(Path));
```

因此flink程序的基本构建是source，transformation和sink

执行时Flink程序映射到dataflows，由流和转换操作组成

## Flink集群有哪些角色？各自作用？

Flink在运行时主要有JM,TM和Client

JM等价于Master，整个集群协调者，负责接收job，协调检查点，故障恢复，管理TM

TM是具体的执行计算的Worker，在上面执行一些Job的Task，单个TM管理其资源，如内存，磁盘，网络，启动时将资源状况向JM汇报

Client是Flink任务提交的客户端，会首先创建，对用户程序进行预处理，提交到集群，Client需要从用户配置中获取JM的地址，并建立JM的连接，将Job提交到JM

Flink 的 Slot的概念

TM是一个JVM进程，会以独立的线程来执行task或者多个subTask，为了控制一个TM能接受多少个task，Flink提出了Task slot的概念

TM会把自己节点上的资源分为不同的slot：固定大小的资源子集。这样避免了不同job的task相互竞争内存资源，但是主要的是slot只会做内存的隔离，没有cpu的隔离。

## Flink常用算子

Map：一对一，DataStream → DataStream

Filter：过滤掉指定条件的数据

KeyBy：按照指定的key进行分组

Reduce：用来进行结果汇总合并

Window：窗口函数，根据某些特性将每个key的数据进行分组（例如：在5s内到达的数据）

## Flink分区策略

分区策略是决定数据如何发向下游，目前有八种

GlobalPartitioner 数据会被分发到下游算子的第一个实例中进行处理。

ShufflePartitioner 数据会被随机分发到下游算子的每一个实例中进行处理。

RebalancePartitioner 数据会被循环发送到下游的每一个实例中进行处理。

RescalePartitioner 这种分区器会根据上下游算子的并行度，循环的方式输出到下游算子的每个实例。这里有点难以理解，假设上游并行度为2，编号为A和B。下游并行度为4，编号为1，2，3，4。那么A则把数据循环发送给1和2，B则把数据循环发送给3和4。假设上游并行度为4，编号为A，B，C，D。下游并行度为2，编号为1，2。那么A和B则把数据发送给1，C和D则把数据发送给2。

BroadcastPartitioner 广播分区会将上游数据输出到下游算子的每个实例中。适合于大数据集和小数据集做Jion的场景。

ForwardPartitioner ForwardPartitioner 用于将记录输出到下游本地的算子实例。它要求上下游算子并行度一样。简单的说，ForwardPartitioner用来做数据的控制台打印。

KeyGroupStreamPartitioner Hash分区器。会将数据按 Key 的 Hash 值输出到下游算子实例中。

CustomPartitionerWrapper 用户自定义分区器。需要用户自己实现Partitioner接口，来定义自己的分区逻辑。例如：

```java
static class CustomPartitioner implements Partitioner<String> {
      @Override
      public int partition(String key, int numPartitions) {
          switch (key){
              case "1":
                  return 1;
              case "2":
                  return 2;
              case "3":
                  return 3;
              default:
                  return 4;
```

## Flink并行度以及设置

Flink任务被分为多个并行任务来执行，每个并行的实例处理一部分数据。这些并行实例的数量被称为并行度。

可以在四个不同层面设置并行度：

操作算子层面

执行环境层面

客户端层面

系统层面

优先级自上而下依次降低

## slot和parallelism有什么区别

slot是指taskmanager的并发执行能力，假设我们将 taskmanager.numberOfTaskSlots 配置为3 那么每一个 
taskmanager 中分配3个 TaskSlot, 3个 taskmanager 一共有9个TaskSlot。

parallelism是指taskmanager实际使用的并发能力。假设我们把 parallelism.default 设置为1，那么9个 TaskSlot 只能用1个，有8个空闲。

## Flink重启策略

固定延迟重启策略（Fixed Delay Restart Strategy）

故障率重启策略（Failure Rate  Restart Strategy）

没有重启策略（no Restart Strategy）

FallBack重启策略（Fallback Restart Strategy）

## Flink分布式缓存

目的是将本地文件缓存到TM中，防止重复拉取

```scala
val env = ExecutionEnvironment.getEnvironment
// register a file from HDFS
env.registerCacheFile（"hdfs:///path/to/your/file", "hdfsFile"）
// register a local executable file (script, executable, ...)
env.registerCachedFile("file:///path/to/exec/file", "localExecFile", true)
 
// define your program and execute
...
val input：DataSet[String] = ...
val result:DataSet[Integer] = input.map(new MyMapper())
...
env.execute()
```

## Flink广播变量

当我们需要访问同一份数据。那么Flink中的广播变量就是为了解决这种情况。

我们可以把广播变量理解为是一个公共的共享变量，我们可以把一个dataset 数据集广播出去，然后不同的task在节点上都能够获取到，这个数据在每个节点上只会存在一份。

## Flink的窗口

Flink 支持两种划分窗口的方式，按照time和count。如果根据时间划分窗口，那么它就是一个time-window 如果根据数据划分窗口，那么它就是一个count-window。

flink支持窗口的两个重要属性（size和interval）

如果size=interval,那么就会形成tumbling-window(无重叠数据)  如果size>interval,那么就会形成sliding-window(有重叠数据) 如果size< interval,  那么这种窗口将会丢失数据。比如每5秒钟，统计过去3秒的通过路口汽车的数据，将会漏掉2秒钟的数据。

通过组合可以得出四种基本窗口：

- time-tumbling-window 无重叠数据的时间窗口，设置方式举例：timeWindow(Time.seconds(5))
- time-sliding-window 有重叠数据的时间窗口，设置方式举例：timeWindow(Time.seconds(5), Time.seconds(3))
- count-tumbling-window无重叠数据的数量窗口，设置方式举例：countWindow(5)
- count-sliding-window 有重叠数据的数量窗口，设置方式举例：countWindow(5,3)

## FLink状态存储

状态的存储、访问以及维护，由一个**可插入**的组件决定，这个组件就叫做**状态后端**（state backend）

**状态后端主要负责两件事：**

 本地(taskmanager)的状态管理

将检查点（checkpoint）状态写入远程存储

Flink提供了三种状态存储方式：

MemoryStateBackend（不稳定，几乎无状态的处理如ETL）

FsStateBackend(本地状态在TaskManager内存, Checkpoint时, 存储在文件系统(hdfs)中)

RocksDBStateBackend(存入本地的RocksDB数据库中,用于超大状态的作业, 例如天级的窗口聚合)

## Flink水印

为了处理 EventTime 窗口计算提出的一种机制, 本质上是一种时间戳。 一般来讲Watermark经常和Window一起被用来处理乱序事件。

## Flink Table & SQL

TableEnvironment是Table API 和 SQL集成的核心概念

主要用来：

​	在内部catalog中注册表

​		Catalog 就是元数据管理中心，其中元数据包括数据库、表、表结构等信息

​		Flink 的 Catalog 相关代码定义在 catalog.java 文件中，是一个 interface

​	注册外部catalog

​	执行sql查询

​	注册用户定义函数

​	将DataStream或者DataSet转换为表

​	持有ExecutionEnvironment或StreamExecutionEnvironment的引用

## Flink SQL原理以及如何解析

Flink SQL解析依赖于Apache calcite

基于此，一次完整的SQL解析过程如下：

- 用户使用对外提供Stream SQL的语法开发业务应用
- 用calcite对StreamSQL进行语法检验，语法检验通过后，转换成calcite的逻辑树节点；最终形成calcite的逻辑计划
- 采用Flink自定义的优化规则和calcite火山模型、启发式模型共同对逻辑树进行优化，生成最优的Flink物理计划
- 对物理计划采用janino codegen生成代码，生成用低阶API DataStream 描述的流应用，提交到Flink平台执行



# Flink 进阶篇

## Flink如何实现流批一体？

Flink中批处理是特殊的流处理，用流处理引擎支持DataSet API 和 DataStream API

## Flink如何做到高效数据交换？

在一个Job中，数据需要在不同的task中交换，交换是由TM负责，TM的网络组件首先从缓存buffer中收集records，然后发送

Records是累积一个批次发送，可以更好利用网络资源

## Fink容错

靠强大的CheckPoint机制和State机制。Checkpoint 负责定时制作分布式快照、对程序中的状态进行备份；State 用来存储计算过程中的中间状态。

### 状态一致性

状态一致性级别：
at-most-once(最多一次): 
at-least-once(至少一次): 
exactly-once(严格一次):

### 端到端的状态一致性

端到端的一致性保证，意味着结果的正确性贯穿了整个流处理应用的始终；每一个组件都保证了它自己的一致性，**整个端到端的一致性级别取决于所有组件中一致性最弱的组件**。

### Checkpoint原理

Flink的分布式快照是根据Chandy-Lamport算法量身定做的。简单来说就是持续创建分布式数据流及其状态的一致快照。

核心思想是在 input source 端插入 barrier，控制 barrier 的同步来实现 snapshot 的备份和 exactly-once 语义。

# Flink 源码篇

