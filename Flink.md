Flink简介



## 1.1简单介绍Flink

Flink 是一个框架和分布式处理引擎，用于对无界和有界数据流进行有状态计算。并且 Flink 提供了数据分布、容错机制以及资源管理等核心功能。Flink提供了诸多高抽象层的API以便用户编写分布式任务：

DataSet API， 对静态数据进行批处理操作，将静态数据抽象成分布式的数据集，用户可以方便地使用Flink提供的各种操作符对分布式数据集进行处理，支持Java、Scala和Python。

DataStream API，对数据流进行流处理操作，将流式的数据抽象成分布式的数据流，用户可以方便地对分布式数据流进行各种操作，支持Java和Scala。

Table API，对结构化数据进行查询操作，将结构化数据抽象成关系表，并通过类SQL的DSL对关系表进行各种查询操作，支持Java和Scala。

此外，Flink 还针对特定的应用领域提供了领域库，例如： Flink ML，Flink 的机器学习库，提供了机器学习Pipelines API并实现了多种机器学习算法。 Gelly，Flink 的图计算库，提供了图计算的相关API及多种图计算算法实现。



## 1.2 Flink跟Spark Streaming的区别

这个问题是一个非常宏观的问题，因为两个框架的不同点非常之多。但是在面试时有非常重要的一点一定要回答出来：**Flink 是标准的实时处理引擎，基于事件驱动。而 Spark Streaming 是微批（Micro-Batch）的模型。**

下面我们就分几个方面介绍两个框架的主要区别：

1）架构模型Spark Streaming 在运行时的主要角色包括：Master、Worker、Driver、Executor，Flink 在运行时主要包含：Jobmanager、Taskmanager和Slot。

2）任务调度Spark Streaming 连续不断的生成微小的数据批次，构建有向无环图DAG，Spark Streaming 会依次创建 DStreamGraph、JobGenerator、JobScheduler。Flink 根据用户提交的代码生成 StreamGraph，经过优化生成 JobGraph，然后提交给 JobManager进行处理，JobManager 会根据 JobGraph 生成 ExecutionGraph，ExecutionGraph 是 Flink 调度最核心的数据结构，JobManager 根据 ExecutionGraph 对 Job 进行调度。

3）时间机制Spark Streaming 支持的时间机制有限，只支持处理时间。 Flink 支持了流处理程序在时间上的三个定义：处理时间、事件时间、注入时间。同时也支持 watermark 机制来处理滞后数据。

4）容错机制对于 Spark Streaming 任务，我们可以设置 checkpoint，然后假如发生故障并重启，我们可以从上次 checkpoint 之处恢复，但是这个行为只能使得数据不丢失，可能会重复处理，不能做到恰好一次处理语义。Flink 则使用两阶段提交协议来解决这个问题。

# 2.Flink快速上手

## 2.1.创建maven项目

POM文件中添加需要的依赖:

```xml

<properties>
    <flink.version>1.12.0</flink.version>
    <java.version>1.8</java.version>
    <scala.binary.version>2.11</scala.binary.version>
    <slf4j.version>1.7.30</slf4j.version>
</properties>

<dependencies>
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-java</artifactId>
        <version>${flink.version}</version>
    </dependency>
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-streaming-java_${scala.binary.version}</artifactId>
        <version>${flink.version}</version>
    </dependency>
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-clients_${scala.binary.version}</artifactId>
        <version>${flink.version}</version>
    </dependency>

    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-runtime-web_${scala.binary.version}</artifactId>
        <version>${flink.version}</version>
    </dependency>

    <dependency>
        <groupId>org.slf4j</groupId>
        <artifactId>slf4j-api</artifactId>
        <version>${slf4j.version}</version>
    </dependency>
    <dependency>
        <groupId>org.slf4j</groupId>
        <artifactId>slf4j-log4j12</artifactId>
        <version>${slf4j.version}</version>
    </dependency>
    <dependency>
        <groupId>org.apache.logging.log4j</groupId>
        <artifactId>log4j-to-slf4j</artifactId>
        <version>2.14.0</version>
    </dependency>
</dependencies>

<build>
    <plugins>
        <plugin>
            <groupId>org.apache.maven.plugins</groupId>
            <artifactId>maven-assembly-plugin</artifactId>
            <version>3.3.0</version>
            <configuration>
                <descriptorRefs>
                    <descriptorRef>jar-with-dependencies</descriptorRef>
                </descriptorRefs>
            </configuration>
            <executions>
                <execution>
                    <id>make-assembly</id>
                    <phase>package</phase>
                    <goals>
                        <goal>single</goal>
                    </goals>
                </execution>
            </executions>
        </plugin>
    </plugins>
</build>
```

src/main/resources添加文件:log4j.properties

```xml
log4j.rootLogger=error, stdout
log4j.appender.stdout=org.apache.log4j.ConsoleAppender
log4j.appender.stdout.layout=org.apache.log4j.PatternLayout
log4j.appender.stdout.layout.ConversionPattern=%-4r [%t] %-5p %c %x - %m%n
```

## 2.2 批处理WordCount

```java
// 1. 创建执行环境
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
// 2. 从文件读取数据  按行读取(存储的元素就是每行的文本)
DataSource<String> lineDS = env.readTextFile("input/words.txt");
// 3. 转换数据格式
FlatMapOperator<String, Tuple2<String, Long>> wordAndOne = lineDS
        .flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] split = line.split(" ");
            for (String word : split) {
                out.collect(Tuple2.of(word, 1L));
            }
        })
        .returns(Types.TUPLE(Types.STRING, Types.LONG));  //当Lambda表达式使用 java 泛型的时候, 由于泛型擦除的存在, 需要显示的声明类型信息

// 4. 按照 word 进行分组
UnsortedGrouping<Tuple2<String, Long>> wordAndOneUG = wordAndOne.groupBy(0);
// 5. 分组内聚合统计
AggregateOperator<Tuple2<String, Long>> sum = wordAndOneUG.sum(1);

// 6. 打印结果
sum.print();
```

## 2.3 流处理WordCount

### 2.3.1 有界流

```java
// 1. 创建流式执行环境
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
// 2. 读取文件
DataStreamSource<String> lineDSS = env.readTextFile("input/words.txt");
// 3. 转换数据格式
SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOne = lineDSS
        .flatMap((String line, Collector<String> words) -> {
            Arrays.stream(line.split(" ")).forEach(words::collect);
        })
        .returns(Types.STRING)
        .map(word -> Tuple2.of(word, 1L))
        .returns(Types.TUPLE(Types.STRING, Types.LONG));
// 4. 分组
KeyedStream<Tuple2<String, Long>, String> wordAndOneKS = wordAndOne
        .keyBy(t -> t.f0);
// 5. 求和
SingleOutputStreamOperator<Tuple2<String, Long>> result = wordAndOneKS
        .sum(1);
// 6. 打印
result.print();
// 7. 执行
env.execute();
```

### 2.3.2 无界流

```java
// 1. 创建流式执行环境
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
// 2. 读取文件
DataStreamSource<String> lineDSS = env.socketTextStream("hadoop102", 9999);
// 3. 转换数据格式
SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOne = lineDSS
        .flatMap((String line, Collector<String> words) -> {
            Arrays.stream(line.split(" ")).forEach(words::collect);
        })
        .returns(Types.STRING)
        .map(word -> Tuple2.of(word, 1L))
        .returns(Types.TUPLE(Types.STRING, Types.LONG));
// 4. 分组
KeyedStream<Tuple2<String, Long>, String> wordAndOneKS = wordAndOne
        .keyBy(t -> t.f0);
// 5. 求和
SingleOutputStreamOperator<Tuple2<String, Long>> result = wordAndOneKS
        .sum(1);
// 6. 打印
result.print();
// 7. 执行
env.execute();
```

## 2.4 总结

```java
//批处理和流处理的执行环境不同
//批处理
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//流处理
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//source不同
//有界流从文件读，无界流从socket或者kafka读
env.readTextFile("input/words.txt");
env.socketTextStream("hadoop102", 9999);
//算子不同
//批处理 VS 流处理
wordAndOne.groupBy(0);
//-------------------
wordAndOne.keyBy(t -> t.f0);
```

# 3.Flink部署

(开发模式，local-cluster模式，standalone模式，yarn模式，scala REPL，K8S & Mesos模式)

## 3.1 开发模式

idea中运行Flink程序的方式就是开发模式。

## 3.2 local-cluster模式

Flink中的Local-cluster(本地集群)模式,主要用于测试, 学习，基本属于**零配置**。解压，上传jar包，启动netcat，在命令行提交应用。

```shell
bin/flink run -m hadoop102:8081 -c com.grantu.flink.java.chapter_2.Flink03_WC_UnBoundedStream ./flink-prepare-1.0-SNAPSHOT.jar
```

在浏览器中查看应用执行情况： http://hadoop1:8081

也可以在log日志查看执行结果：

```shell
cat flink-grant-taskexecutor-0-hadoop102.out
```

也可以在WEB UI提交应用

```scala
 val result = sc.textFile("input").flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).collect().mkString(",")
```

## 3.3 standalone模式

修改配置文件:flink-conf.yaml

```shell
jobmanager.rpc.address: hadoop1
```

修改配置文件:workers： 

```scala
hadoop102
hadoop103
hadoop104
```

分发flink-standalone到其他节点

启动standalone集群

```shell
bin/start-cluster.sh / bin/stop-cluster.sh
```



## 3.4 Standalone 高可用（HA）

任何时候都有一个 **主 JobManager** 和多个**备用 JobManagers**，以便在主节点失败时有备用 JobManagers 来接管集群。这保证了没有单点故障，一旦备 JobManager 接管集群，作业就可以正常运行。**主备 JobManager 实例之间没有明显的区别**。每个 JobManager 都可以充当主备节点。

1. 修改配置文件: flink-conf.yaml

```shell
high-availability: zookeeper

high-availability.storageDir: hdfs://hadoop102:8020/flink/standalone/ha

high-availability.zookeeper.quorum: hadoop102:2181,hadoop103:2181,hadoop104:2181

high-availability.zookeeper.path.root: /flink-standalone

high-availability.cluster-id: /cluster_grant
```

2. 修改配置文件: masters

```
hadoop102:8081

hadoop103:8081
```

3. 分发修改的后配置文件到其他节点

4. 在/etc/profile.d/my.sh中配置环境变量

```shell
export HADOOP_CLASSPATH=`hadoop classpath
```

**注意:** 

需要提前**保证HAOOP_HOME环境变量配置成功

分发到其他节点

**5.** **首先启动dfs集群和zookeeper集群**

6. 启动standalone HA集群

bin/start-cluster.sh

7. 可以分别访问

http://hadoop102:8081

http://hadoop103:8081

8. 在zkCli.sh中查看谁是leader

```shell
get /flink-standalone/cluster_grantu/leader/rest_server_lock
```

杀死hadoop102上的Jobmanager, 再看leader

**注意:** 不管是不是leader从WEB UI上看不到区别, **并且都可以与之提交应用**.



# 4. Flink运行架构

## 4.1 运行架构

https://ci.apache.org/projects/flink/flink-docs-release-1.11/fig/processes.svg

Flink运行时包含2种进程:1个**JobManager**和至少1个**TaskManager**

### 4.1.1 客户端

客户端是用于准备和发送dataflow到JobManager. 

然后客户端可以断开与JobManager的连接(detached mode), 也可以继续保持与JobManager的连接(attached mode)

客户端作为触发执行的Java或者scala代码的一部分运行, 也可以在命令行运行:

```shell
bin/flink run ...
```



### 4.1.2 JobManager

控制应用程序执行主进程，每个应用程序都会被一个JobManager所控制执行。

JobManager会先接收到要执行的应用程序，这个应用程序会包括：

作业图（JobGraph）

逻辑数据流图（logical dataflow graph）

打包了所有的类、库和其它资源的JAR包

JobManager把JobGraph转换成一个物理层面的数据流图，这个图被叫做“执行图”（ExecutionGraph），包含了所有可以并发执行的任务。

JobManager会向资源管理器（ResourceManager）请求执行任务必要的资源，也就是任务管理器（TaskManager）上的插槽（slot）。

一旦它获取到了足够的资源，就会将执行图分发到真正运行它们的TaskManager上。

而在运行过程中，JobManager会负责所有需要中央协调的操作，比如说检查点（checkpoints）的协调。

**JobManager 进程包含3个不同的组件**

#### 4.1.2.1 ResourceManager

负责资源的管理，在整个 Flink 集群中只有一个 ResourceManager. 

**注意这个ResourceManager不是Yarn中的ResourceManager**

主要负责管理任务管理器（TaskManager）的插槽（slot），TaskManger插槽是Flink中定义的处理资源单元。

当JobManager申请插槽资源时，ResourceManager会将有空闲插槽的TaskManager分配给JobManager。如果ResourceManager没有足够的插槽来满足JobManager的请求，它还可以向资源提供平台发起会话，以提供启动TaskManager进程的容器。另外，ResourceManager还负责终止空闲的TaskManager，释放计算资源。

#### 4.1.2.2 Dispatcher

负责接收用户提供的作业，并且负责为这个新提交的作业启动一个新的JobManager 组件. Dispatcher也会启动一个Web UI，用来方便地展示和监控作业执行的信息。Dispatcher在架构中可能并不是必需的，这取决于应用提交运行的方式。

#### 4.1.2.3 JobMaster

JobMaster负责管理单个JobGraph的执行.多个Job可以同时运行在一个Flink集群中, 每个Job都有一个自己的JobMaster.

### 4.1.3 TaskManager

Flink中的工作进程。通常在Flink中会有多个TaskManager运行，每一个TaskManager都包含了一定数量的插槽（slots）。插槽的数量限制了TaskManager能够执行的任务数量。

启动之后，TaskManager会向资源管理器注册它的插槽；收到资源管理器的指令后，TaskManager就会将一个或者多个插槽提供给JobManager调用。JobManager就可以向插槽分配任务（tasks）来执行了。
在执行过程中，一个TaskManager可以跟其它运行同一应用程序的TaskManager交换数据。

## 4.2 核心概念

### 4.2.1 TaskManager与Slots

Flink中每一个worker(TaskManager)都是一个JVM进程，它可能会在独立的线程上执行一个Task。为了控制一个worker能接收多少个task，worker通过**Task Slot**来进行控制（一个worker至少有一个Task Slot）。

### 4.2.2 Parallelism（并行度）

算子的子任务（subtask）个数被称为算子并行度（parallelism），一个流程序的并行度，认为是所有算子中最大的并行度。一个程序中，不同算子可能具有不同并行度

Stream在算子之间传输数据的形式可以是one-to-one(forwarding)的模式也可以是redistributing的模式，取决于算子种类。

 **One-to-one：**
stream(比如在source和map operator之间)维护着分区以及元素的顺序。flatMap 算子的子任务看到的元素的个数以及顺序跟source 算子的子任务生产的元素的个数、顺序相同，map、filter、flatMap等算子都是one-to-one的对应关系。类似于spark中的窄依赖

 **Redistributing：**
stream(map()跟keyBy/window之间或者keyBy/window跟sink之间)的分区会发生改变。每一个算子的子任务依据所选择的transformation发送数据到不同的目标任务。例如，keyBy()基于hashCode重分区、broadcast和rebalance会随机重新分区，这些算子都会引起redistribute过程，而redistribute过程就类似于Spark中的shuffle过程。类似于spark中的宽依赖

### 4.2.3 Task与SubTask

一个算子就是一个Task. 一个算子的并行度是几, 这个Task就有几个SubTask

### 4.2.4 Operator Chains（任务链）

相同并行度的one to one操作，Flink将这样相连的算子链接在一起形成一个task，原来的算子成为里面的一部分， 每个task被一个线程执行

**将算子链接成task是非常有效的优化：它能减少线程之间的切换和基于缓存区的数据交换，在减少时延的同时提升吞吐量。链接的行为可以在编程API中进行指定。**

### 4.2.5 ExecutionGraph（执行图）

由Flink程序直接映射成的数据流图是StreamGraph，也被称为逻辑流图，表示计算逻辑的高级视图。为了执行一个流处理程序，Flink需要将逻辑流图转换为物理数据流图（也叫执行图）

Flink 中的执行图可以分成四层：

StreamGraph -> JobGraph -> ExecutionGraph -> Physical Graph

**StreamGraph：**
是根据用户通过 Stream API 编写的代码生成的最初的图。用来表示程序的拓扑结构。

**JobGraph：**
StreamGraph经过优化后生成了 JobGraph，是提交给 JobManager 的数据结构。主要的优化为: 将多个符合条件的节点 chain 在一起作为一个节点，这样可以减少数据在节点之间流动所需要的序列化/反序列化/传输消耗。

**ExecutionGraph：**
JobManager 根据 JobGraph 生成ExecutionGraph。ExecutionGraph是JobGraph的并行化版本，是调度层最核心的数据结构。

**Physical Graph：**
JobManager 根据 ExecutionGraph 对 Job 进行调度后，在各个TaskManager 上部署 Task 后形成的“图”，并不是一个具体的数据结构。

## 4.3 提交流程

### 4.3.1 高级视角提交流程(通用提交流程)

当一个应用提交执行时，Flink的各个组件是如何交互协作的：

### 4.3.2 yarn-cluster提交流程per-job

1.Flink任务提交后，Client向HDFS上传Flink的Jar包和配置

2.向Yarn ResourceManager提交任务，ResourceManager分配Container资源

3.通知对应的NodeManager启动ApplicationMaster，ApplicationMaster启动后加载Flink的Jar包和配置构建环境，然后启动JobManager

4.ApplicationMaster向ResourceManager申请资源启动TaskManager

5.ResourceManager分配Container资源后，由ApplicationMaster通知资源所在节点的NodeManager启动TaskManager

6.NodeManager加载Flink的Jar包和配置构建环境并启动TaskManager

7.TaskManager启动后向JobManager发送心跳包，并等待JobManager向其分配任务。

# 5.Flink流处理核心编程

和其他所有的计算框架一样，Flink也有一些基础的开发步骤以及基础，核心的API，从开发步骤的角度来讲，主要分为四大部分：

Environment---->Source---->Transform---->Sink

## 5.1 Environment 

Flink Job在提交执行计算时，需要首先建立和Flink框架之间的联系，也就指的是当前的flink运行环境，只有获取了环境信息，才能将task调度到不同的taskManager执行。

```scala
// 批处理环境
ExecutionEnvironment benv = ExecutionEnvironment.getExecutionEnvironment();
// 流式数据处理环境
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
```

## 5.2 Source

### 5.2.1

导入注解工具依赖, 方便生产POJO类

```xml
<!-- https://mvnrepository.com/artifact/org.projectlombok/lombok -->
<dependency>
    <groupId>org.projectlombok</groupId>
    <artifactId>lombok</artifactId>
    <version>1.18.16</version>
    <scope>provided</scope>
</dependency>
```

准备一个WaterSensor类方便演示:

```java
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 水位传感器：用于接收水位数据
 *
 * id:传感器编号
 * ts:时间戳
 * vc:水位
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class WaterSensor {
    private String id;
    private Long ts;
    private Integer vc;
}
```

从Java集合中读取数据：

```java
        List<WaterSensor> waterSensors = Arrays.asList(
          new WaterSensor("ws_001", 1577844001L, 45),
          new WaterSensor("ws_002", 1577844015L, 43),
          new WaterSensor("ws_003", 1577844020L, 42));

        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
          .fromCollection(waterSensors)
         .print();
        env.execute();
```

### 5.2.2 从文件读取数据

```java
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
          .readTextFile("input")
          .print();
        env.execute();
```

**说明:**

1. 参数可以是目录也可以是文件

2. 路径可以是相对路径也可以是绝对路径

3. 相对路径是从系统属性user.dir获取路径: idea下是project的根目录, standalone模式下是集群节点根目录

4. 也可以从hdfs目录下读取, 使用路径:hdfs://...., 由于Flink没有提供hadoop相关依赖, 需要pom中添加相关依赖:

```xml
<dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-client</artifactId>
    <version>3.1.3</version>
   <scope>provided</scope>
</dependency>
```

### 5.2.3  从socket读取数据

### 5.2.4  从Kafka读取数据（重点）

**添加相应的依赖**

```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-connector-kafka_2.11</artifactId>
    <version>1.12.0</version>
</dependency>
```

参考代码

```java
        // 0.Kafka相关配置
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        properties.setProperty("group.id", "Flink01_Source_Kafka");
        properties.setProperty("auto.offset.reset", "latest");

        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
          .addSource(new FlinkKafkaConsumer<>("sensor", new SimpleStringSchema(), properties))
          .print("kafka source");
        env.execute();
```



### 5.2.5 自定义source

大多数情况下，前面的数据源已经能够满足需要，但是难免会存在特殊情况的场合，所以flink也提供了能**自定义数据源**的方式.

**具体来讲，自定义source需要实现SourceFunction<T>，重写其中的run方法和cancel方法。**

**run方法读取数据或者是获取数据，cancel方法一般来停止run方法。 大多数source在run方法内部都会有一个while循环,当调用cancel, 应该可以让run方法中的while循环结束**



```java
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
          .addSource(new MySource("hadoop102", 9999))
          .print();
        env.execute();
    }
    public static class MySource implements SourceFunction<WaterSensor> {
        private String host;
        private int port;
        private volatile boolean isRunning = true;
        private Socket socket;
        public MySource(String host, int port) {
            this.host = host;
            this.port = port;
        }
        @Override
        public void run(SourceContext<WaterSensor> ctx) throws Exception {
            // 实现一个从socket读取数据的source
            socket = new Socket(host, port);
            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
            String line = null;
            while (isRunning && (line = reader.readLine()) != null) {
                String[] split = line.split(",");
                ctx.collect(new WaterSensor(split[0], Long.valueOf(split[1]), Integer.valueOf(split[2])));
            }
        }
        /**
         * 大多数的source在run方法内部都会有一个while循环,
         * 当调用这个方法的时候, 应该可以让run方法中的while循环结束
         */
        @Override
        public void cancel() {
            isRunning = false;
            try {
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
```

## 5.3 Transform

转换算子可以把一个或多个DataStream转成一个新的DataStream.程序可以把多个复杂的转换组合成复杂的数据流拓扑。

#### 5.3.1 map

5.3.1map（rich版本可以优化多输入输出的情况，例如与外部建立连接可以减少次数）
作用
将数据流中的数据进行转换, 形成新的数据流，消费一个元素并产出一个元素

参数
lambda表达式或MapFunction实现类
返回
DataStream → DataStream

示例
得到一个新的数据流: 新的流的元素是原来流的元素的平方

**匿名内部类对象**

```java
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

```

```
env
  .fromElements(1, 2, 3, 4, 5)
  .map(new MapFunction<Integer, Integer>() {
      @Override
      public Integer map(Integer value) throws Exception {
          return value * value;
      }
  })
  .print();

env.execute();
```

**Lambda表达式表达式**

```java
env
  .fromElements(1, 2, 3, 4, 5)
  .map(ele -> ele * ele)
  .print();
```


 **静态内部类**

```java
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
public class Flink01_TransForm_Map_StaticClass {
public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env
  .fromElements(1, 2, 3, 4, 5)
  .map(new MyMapFunction())
  .print();
env.execute();
    public static class MyMapFunction implements MapFunction<Integer, Integer> {
        @Override
public Integer map(Integer value) throws Exception {
    return value * value;

```



**Rich Function类**
所有Flink函数类都有其Rich版本。

它与常规函数的不同在于，可以获取运行环境的上下文，并拥有一些生命周期方法，所以可以实现更复杂的功能。也有意味着提供了更多的，更丰富的功能。例如：RichMapFunction

```java
public class Flink01_TransForm_Map_RichMapFunction {
public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(5);
    env
  .fromElements(1, 2, 3, 4, 5)
  .map(new MyRichMapFunction()).setParallelism(2)
  .print();

env.execute();
    public static class MyRichMapFunction extends RichMapFunction<Integer, Integer> {
    // 默认生命周期方法, 初始化方法, 在每个并行度上只会被调用一次
    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("open ... 执行一次");
    }
        // 默认生命周期方法, 最后一个方法, 做一些清理工作, 在每个并行度上只调用一次
@Override
public void close() throws Exception {
    System.out.println("close ... 执行一次");
}

@Override
public Integer map(Integer value) throws Exception {
    System.out.println("map ... 一个元素执行一次");
    return value * value;
```



1.默认生命周期方法, 初始化方法, 在每个并行度上只会被调用一次, 而且先被调用

2.默认生命周期方法, 最后一个方法, 做一些清理工作, 在每个并行度上只调用一次, 而且是最后被调用

3. getRuntimeContext()方法提供了函数的RuntimeContext的一些信息，例如函数执行的并行度，任务的名字，以及state状态. 开发人员在需要的时候自行调用获取运行时上下文对象.

### 5.3.2 flatMap

（可以替换map和filter）

作用 ：消费一个元素并产生零个或多个元素

参数 ： FlatMapFunction 实现类

返回 ：DataStream → DataStream

示例

  **匿名内部类写法**

```java
// 新的流存储每个元素的平方和3次方
env
  .fromElements(1, 2, 3, 4, 5)
  .flatMap(new FlatMapFunction<Integer, Integer>() {
      @Override
      public void flatMap(Integer value, Collector<Integer> out) throws Exception {
          out.collect(value * value);
          out.collect(value * value * value);
      }
  })
  .print();
//Lambda表达式写法
env
  .fromElements(1, 2, 3, 4, 5)
  .flatMap((Integer value, Collector<Integer> out) -> {
      out.collect(value * value);
      out.collect(value * value * value);
  }).returns(Types.INT)
  .print();
```


说明: 在使用Lambda表达式表达式的时候, 由于泛型擦除的存在, 在运行的时候无法获取泛型的具体类型, 全部当做Object来处理, 及其低效, 所以Flink要求当参数中有泛型的时候, 必须明确指定泛型的类型.

### 5.3.3 filter

作用
根据指定的规则将满足条件（true）的数据保留，不满足条件(false)的数据丢弃

参数
FlatMapFunction实现类

返回
DataStream → DataStream

示例
  匿名内部类写法

```java
// 保留偶数, 舍弃奇数
env
  .fromElements(10, 3, 5, 9, 20, 8)
  .filter(new FilterFunction<Integer>() {
      @Override
      public boolean filter(Integer value) throws Exception {
          return value % 2 == 0;
      }
  })
  .print();
```

Lambda表达式写法

```java
env
  .fromElements(10, 3, 5, 9, 20, 8)
  .filter(value -> value % 2 == 0)
  .print();
```



### 5.3.4 keyBy

90%的key为字符串

作用
把流中的数据分到不同的分区(并行度)中.具有相同key的元素会分到同一个分区中.一个分区中可以有多重不同的key.

**决定数据进入哪个并行度**

在内部是使用的是key的hash分区来实现的.

参数
Key选择器函数: interface KeySelector<IN, KEY>
注意: 什么值不可以作为KeySelector的Key:

1. 没有覆写hashCode方法的POJO, 而是依赖Object的hashCode. 因为这样分组没有任何的意义: 每个元素都会得到一个独立无二的组.  实际情况是:可以运行, 但是分的组没有意义.

2. 任何类型的数组

返回
	DataStream → KeyedStream

示例
  匿名内部类写法

```java
// 奇数分一组, 偶数分一组
env
  .fromElements(10, 3, 5, 9, 20, 8)
  .keyBy(new KeySelector<Integer, String>() {
      @Override
      public String getKey(Integer value) throws Exception {
          return value % 2 == 0 ? "偶数" : "奇数";
      }
  })
  .print();
env.execute();
//Lambda表达式写法
env
  .fromElements(10, 3, 5, 9, 20, 8)
  .keyBy(value -> value % 2 == 0 ? "偶数" : "奇数")
  .print();

```



### 5.3.5 shuffle

作用
把流中的元素随机打乱. 对同一个组数据, 每次执行得到的结果都不同.

**参数 无**
返回
DataStream → DataStream

示例

```java
env
  .fromElements(10, 3, 5, 9, 20, 8)
  .shuffle()
```

**5.3.6 split和select**

已经过时, 在1.12中已经被移除

作用
在某些情况下，我们需要将数据流根据某些特征拆分成两个或者多个数据流，给不同数据流增加标记以便于从流中取出.

split用于给流中的每个元素添加标记. select用于根据标记取出对应的元素, 组成新的流.

参数
split参数: interface OutputSelector<OUT>
select参数: 字符串
返回
split: SingleOutputStreamOperator -> SplitStream
slect: SplitStream -> DataStream

### 5.3.7 connect

作用
在某些情况下，我们需要将两个不同来源的数据流进行连接，实现数据匹配，比如订单支付和第三方交易信息，这两个信息的数据就来自于不同数据源，连接后，将订单支付和第三方交易信息进行对账，此时，才能算真正的支付完成。
Flink中的connect算子可以连接两个保持他们类型的数据流，两个数据流被connect之后，只是被放在了一个同一个流中，内部依然保持各自的数据和形式不发生任何变化，两个流相互独立。

参数
另外一个流

返回
DataStream[A], DataStream[B] -> ConnectedStreams[A,B]

示例：

```java
DataStreamSource<Integer> intStream = env.fromElements(1, 2, 3, 4, 5);
DataStreamSource<String> stringStream = env.fromElements("a", "b", "c");
// 把两个流连接在一起: 貌合神离
ConnectedStreams<Integer, String> cs = intStream.connect(stringStream);
cs.getFirstInput().print("first");
cs.getSecondInput().print("second");
env.execute();

```

**注意:**
**1.两个流中存储的数据类型可以不同**
**2.只是机械的合并在一起, 内部仍然是分离的2个流**
**3.只能2个流进行connect, 不能有第3个参与**

**5.3.8 union**

作用
对两个或者两个以上的DataStream进行union操作，产生一个包含所有DataStream元素的新DataStream

示例

```java
DataStreamSource<Integer> stream1 = env.fromElements(1, 2, 3, 4, 5);
DataStreamSource<Integer> stream2 = env.fromElements(10, 20, 30, 40, 50);
DataStreamSource<Integer> stream3 = env.fromElements(100, 200, 300, 400, 500);

// 把多个流union在一起成为一个流, 这些流中存储的数据类型必须一样: 水乳交融
stream1
  .union(stream2)
  .union(stream3)
  .print();
```

**connect与 union 区别：**
	1.union之前两个或多个流的**类型必须是一样**，connect可以不一样
	2.connect只能操作两个流，**union可以操作多个**。

**5.3.9 简单滚动聚合算子**

常见的滚动聚合算子

**sum, min,max**
**minBy,maxBy**

作用
KeyedStream的每一个支流做聚合。执行完成后，会将聚合的结果合成一个流返回，所以结果都是DataStream

参数
如果流中存储的是POJO或者scala的样例类, 参数使用字段名.  如果流中存储的是元组, 参数就是位置(基于0...)

返回
KeyedStream -> SingleOutputStreamOperator

示例
示例1

```java
DataStreamSource<Integer> stream = env.fromElements(1, 2, 3, 4, 5);
KeyedStream<Integer, String> kbStream = stream.keyBy(ele -> ele % 2 == 0 ? "奇数" : "偶数");
kbStream.sum(0).print("sum");
kbStream.max(0).print("max");
kbStream.min(0).print("min");
```


示例2

```java
ArrayList<WaterSensor> waterSensors = new ArrayList<>();
waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 30));
waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));

KeyedStream<WaterSensor, String> kbStream = env
  .fromCollection(waterSensors)
  .keyBy(WaterSensor::getId);
kbStream
  .sum("vc")
  .print("maxBy...");
```



**注意:** 
	分组聚合后, 理论上只能取分组字段和聚合结果, 但是Flink允许其他的字段也可以取出来, 其他字段默认情况是取的是这个组内第一个元素的字段值

  示例3:

```java
ArrayList<WaterSensor> waterSensors = new ArrayList<>();
waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 50));
waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));

KeyedStream<WaterSensor, String> kbStream = env
  .fromCollection(waterSensors)
  .keyBy(WaterSensor::getId);

kbStream
  .maxBy("vc", false)
```

**注意:** 
maxBy和minBy可以指定当出现相同值的时候,其他字段是否取第一个. true表示取第一个, false表示取与最大值(最小值)同一行的.

### 5.3.10  reduce

**作用**
一个分组数据流的聚合操作，合并当前的元素和上次聚合的结果，产生一个新的值，返回的流中包含每一次聚合的结果，而不是只返回最后一次聚合的最终结果。
为什么还要把中间值也保存下来? 考虑流式数据的特点: 没有终点, 也就没有最终的概念了. 任何一个中间的聚合结果都是值!

参数
interface ReduceFunction<T>

返回
KeyedStream -> SingleOutputStreamOperator

示例
  匿名内部类写法

```java
ArrayList<WaterSensor> waterSensors = new ArrayList<>();
waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 50));
waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));

KeyedStream<WaterSensor, String> kbStream = env
  .fromCollection(waterSensors)
  .keyBy(WaterSensor::getId);

kbStream
  .reduce(new ReduceFunction<WaterSensor>() {
      @Override
      public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
          System.out.println("reducer function ...");
          return new WaterSensor(value1.getId(), value1.getTs(), value1.getVc() + value2.getVc());
      }
  })
  .print("reduce...");

env.execute();
//Lambda表达式写法
kbStream
  .reduce((value1, value2) -> {
      System.out.println("reducer function ...");
      return new WaterSensor(value1.getId(), value1.getTs(), value1.getVc() + value2.getVc());
```


**注意:** 
1.聚合后结果的类型, 必须和原来流中元素的类型保持一致!

### 5.3.11 process

做相应处理时每个并行度内统一执行，例如求和，最终结果有两个，每个并行度一个结果

**Process Function中定义的属性作用域是并行度**

**上下文对象可以获取当前key**

作用
process算子在Flink算是一个比较底层的算子, 很多类型的流上都可以调用, 可以从流中获取更多的信息(不仅仅数据本身)

示例1: 在keyBy之前的流上使用

```java
env
  .fromCollection(waterSensors)
  .process(new ProcessFunction<WaterSensor, Tuple2<String, Integer>>() {
      @Override
      public void processElement(WaterSensor value,
                                 Context ctx,
                                 Collector<Tuple2<String, Integer>> out) throws Exception {
          out.collect(new Tuple2<>(value.getId(), value.getVc()));
```

示例2: 在keyBy之后的流上使用

```java
env
  .fromCollection(waterSensors)
  .keyBy(WaterSensor::getId)
  .process(new KeyedProcessFunction<String, WaterSensor, Tuple2<String, Integer>>() {
      @Override
      public void processElement(WaterSensor value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
          out.collect(new Tuple2<>("key是:" + ctx.getCurrentKey(), value.getVc()));
```



### 5.3.12对流重新分区的几个算子

####  KeyBy

先按照key分组, 按照key的双重hash来选择后面的分区

####  shuffle

对流中的元素随机分区
	

#### rebalance

对流中的元素平均分布到每个区.当处理倾斜数据的时候, 进行性能优化

#### rescale

同 rebalance一样, 也是平均循环的分布数据. 但是要比rebalance更高效, 因为rescale不需要通过网络, 完全走的"管道"

## 5.4 Sink

在Flink中Sink表示为将数据存储起来的意思，也可以将范围扩大，表示将处理完的数据发送到指定的存储系统的输出操作.

print方法其实就是一种Sink

```java
public DataStreamSink<T> print(String sinkIdentifier) {
   PrintSinkFunction<T> printFunction = new PrintSinkFunction<>(sinkIdentifier, false);
   return addSink(printFunction).name("Print to Std. Out");
```


Flink内置了一些Sink, 除此之外的Sink需要用户自定义!

### 5.4.1 KafkaSink

**添加Kafka Connector依赖**

```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-connector-kafka_2.11</artifactId>
    <version>1.11.2</version>
</dependency>
<dependency>
    <groupId>com.alibaba</groupId>
    <artifactId>fastjson</artifactId>
    <version>1.2.75</version>
</dependency>
```

启动Kafka集群
Sink到Kafka的示例代码

```java
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.ArrayList;

  public class Flink01_Sink_Kafka {
  public static void main(String[] args) throws Exception {
      ArrayList<WaterSensor> waterSensors = new ArrayList<>();
      waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
      waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
      waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 50));
      waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
      waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));
```

```
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
env
  .fromCollection(waterSensors)
  .map(JSON::toJSONString)
  .addSink(new FlinkKafkaProducer<String>("hadoop102:9092", "topic_sensor", new SimpleStringSchema()));
```



在Linux启动一个消费者, 查看是否收到数据

```shell
bin/kafka-console-consumer.sh --bootstrap-server hadoop102:9092 --topic topic_sensor
```



### 5.4.2 RedisSink

添加Redis Connector依赖

```xml
<!-- https://mvnrepository.com/artifact/org.apache.flink/flink-connector-redis -->
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-connector-redis_2.11</artifactId>
    <version>1.1.5</version>
</dependency>
```

启动Redis服务器
Sink到Redis的示例代码

```java
import com.alibaba.fastjson.JSON;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.util.ArrayList;

  public class Flink02_Sink_Redis {
  public static void main(String[] args) throws Exception {
      ArrayList<WaterSensor> waterSensors = new ArrayList<>();
      waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
      waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
      waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 50));
      waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
      waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));
```

```
// 连接到Redis的配置
FlinkJedisPoolConfig redisConfig = new FlinkJedisPoolConfig.Builder()
  .setHost("hadoop102")
  .setPort(6379)
  .setMaxTotal(100)
  .setTimeout(1000 * 10)
  .build();
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
env
  .fromCollection(waterSensors)
  .addSink(new RedisSink<>(redisConfig, new RedisMapper<WaterSensor>() {
      /*
        key                 value(hash)
        "sensor"            field           value
                            sensor_1        {"id":"sensor_1","ts":1607527992000,"vc":20}
                            ...             ...
       */

      @Override
      public RedisCommandDescription getCommandDescription() {
          // 返回存在Redis中的数据类型  存储的是Hash, 第二个参数是外面的key
          return new RedisCommandDescription(RedisCommand.HSET, "sensor");
      }

      @Override
      public String getKeyFromData(WaterSensor data) {
          // 从数据中获取Key: Hash的Key
          return data.getId();
      }

      @Override
      public String getValueFromData(WaterSensor data) {
          // 从数据中获取Value: Hash的value
          return JSON.toJSONString(data);
      }
  }));

env.execute();
```


Redis查看是否收到数据
redis-cli --raw

注意: 
发送了5条数据, Redis中只有2条数据. 原因是hash的field的重复了, 后面的会把前面的覆盖掉

### 5.4.3 ElasticsearchSink

  添加Elasticsearch Connector依赖

```xml
<!-- https://mvnrepository.com/artifact/org.apache.flink/flink-connector-elasticsearch6 -->
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-connector-elasticsearch6_2.11</artifactId>
    <version>1.12.0</version>
</dependency>
```

启动Elasticsearch集群
Sink到Elasticsearch的示例代码

```java
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

  public class Flink03_Sink_ES {
  public static void main(String[] args) throws Exception {
      ArrayList<WaterSensor> waterSensors = new ArrayList<>();
      waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
      waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
      waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 50));
      waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
      waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));
```

```
List<HttpHost> esHosts = Arrays.asList(
  new HttpHost("hadoop102", 9200),
  new HttpHost("hadoop103", 9200),
  new HttpHost("hadoop104", 9200));
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
env
  .fromCollection(waterSensors)
  .addSink(new ElasticsearchSink.Builder<WaterSensor>(esHosts, new ElasticsearchSinkFunction<WaterSensor>() {

      @Override
      public void process(WaterSensor element, RuntimeContext ctx, RequestIndexer indexer) {
          // 1. 创建es写入请求
          IndexRequest request = Requests
            .indexRequest("sensor")
            .type("_doc")
            .id(element.getId())
            .source(JSON.toJSONString(element), XContentType.JSON);
          // 2. 写入到es
          indexer.add(request);
      }
  }).build());

env.execute();
```


Elasticsearch查看是否收到数据

**注意**

如果出现如下错误: 

添加log4j2的依赖:

```xml
<dependency>
    <groupId>org.apache.logging.log4j</groupId>
    <artifactId>log4j-to-slf4j</artifactId>
    <version>2.14.0</version>
</dependency>
```


  如果是无界流, 需要配置bulk的缓存

```java
esSinkBuilder.setBulkFlushMaxActions(1);
```



### 5.4.4自定义Sink

自定义一个到Mysql的Sink

**需要：**

调用addSink方法，传入RichSinkFunction，需要创建对象。

重写open，close，invoke方法。其中invoke方法来获取值

在Mysql中创建数据库和表

```sql
create database test;
use test;
CREATE TABLE `sensor` (
  `id` varchar(20) NOT NULL,
  `ts` bigint(20) NOT NULL,
  `vc` int(11) NOT NULL,
  PRIMARY KEY (`id`,`ts`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
```

导入Mysql驱动

```xml
<dependency>
    <groupId>mysql</groupId>
    <artifactId>mysql-connector-java</artifactId>
    <version>5.1.49</version>
</dependency>
```


写到Mysql的自定义Sink示例代码 

```java
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
  public class Flink04_Sink_Custom {
  public static void main(String[] args) throws Exception {
      ArrayList<WaterSensor> waterSensors = new ArrayList<>();
      waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
      waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
      waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 50));
      waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
      waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        env.fromCollection(waterSensors)
          .addSink(new RichSinkFunction<WaterSensor>() {

              private PreparedStatement ps;
              private Connection conn;

              @Override
              public void open(Configuration parameters) throws Exception {
                  conn = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test?useSSL=false", "root", "aaaaaa");
                  ps = conn.prepareStatement("insert into sensor values(?, ?, ?)");
              }

              @Override
              public void close() throws Exception {
                  ps.close();
                  conn.close();
              }

              @Override
              public void invoke(WaterSensor value, Context context) throws Exception {
                  ps.setString(1, value.getId());
                  ps.setLong(2, value.getTs());
                  ps.setInt(3, value.getVc());
                  ps.execute();
```

​    

## 5.5 执行模式(Execution Mode)



Flink在1.12.0上对流式API新增一项特性:可以根据你的使用情况和Job的特点, 可以选择不同的运行时执行模式(runtime execution modes).

流式API的传统执行模式我们称之为STREAMING 执行模式, 这种模式一般用于无界流, 需要持续的在线处理

1.12.0新增了一个BATCH执行模式, 这种执行模式在执行方式上类似于MapReduce框架. 这种执行模式一般用于有界数据.

默认是使用的STREAMING 执行模式

### 5.5.1 选择执行模式

BATCH执行模式仅仅用于有界数据, 而STREAMING 执行模式可以用在有界数据和无界数据.

一个公用的规则就是: 当你处理的数据是有界的就应该使用BATCH执行模式, 因为它更加高效. 当你的数据是无界的, 则必须使用STREAMING 执行模式, 因为只有这种模式才能处理持续的数据流.

### 5.5.2 配置BATH执行模式

执行模式有3个选择可配:

1 STREAMING(默认)

2 BATCH

3 AUTOMATIC

通过命令行配置

```shell
bin/flink run -Dexecution.runtime-mode=BATCH ...
```

通过代码配置

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setRuntimeMode(RuntimeExecutionMode.BATCH);
```

**建议: 不要在运行时配置(代码中配置), 而是使用命令行配置, 引用这样会灵活: 同一个应用即可以用于无界数据也可以用于有界数据**

### 5.5.3有界数据用STREAMING和BATCH的区别



STREAMING模式下, 数据是来一条输出一次结果.

BATCH模式下, 数据处理完之后, 一次性输出结果.

下面展示WordCount的程序读取文件内容在不同执行模式下的执行结果对比:

流式模式

```java
// 默认流式模式, 可以不用配置
env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
```

批处理模式

```java
env.setRuntimeMode(RuntimeExecutionMode.BATCH);
```

自动模式

```
env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
```

**注意:** 
1.12.0版批模式下, 如果单词只有1个, 则不会输出, 原因是因为没有保存状态. 关于状态的概念后面再讲.

# 6.Flink流处理核心编程实战

## 6.1  基于埋点日志数据的网络流量统计

### 6.1.1  网站总浏览量（PV）的统计

​	衡量网站流量一个最简单的指标，就是网站的页面浏览量（Page View，**PV**）。用户每次打开一个页面便记录1次PV，多次打开同一页面则浏览量累计。

​	一般来说，PV与来访者的数量成正比，但是PV并不直接决定页面的真实来访者数量，如同一个来访者通过不断的刷新页面，也可以制造出非常高的PV。接下来我们就用咱们之前学习的Flink算子来实现PV的统计。

将数据用JavaBean类封装

```java
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class UserBehavior {
    private Long userId;
    private Long itemId;
    private Integer categoryId;
    private String behavior;
    private Long timestamp;
}
```



**PV实现思路一：WordCount**

读数据-->数据按行切分-->每行元素封装进Bean对象对应属性-->过滤访问数据-->每一次访问封装成tuple（页面，1）类型-->按照页面分类-->将访问次数求和

```java
//创建环境部分省略，创建流处理环境
        env.readTextFile("input/UserBehavior.csv")
                .map(line ->{
                    String[] split = line.split(",");
                    return new UserBehavior( Long.valueOf(split[0]),
                            Long.valueOf(split[1]),
                            Integer.valueOf(split[2]),
                            split[3],
                            Long.valueOf(split[4]));
                }).filter(behavior -> "pv".equals(behavior.getBehavior()))
                .map(behavior -> Tuple2.of("PV",1L)).returns(Types.TUPLE(Types.STRING,Types.LONG))
                .keyBy(value -> value.f0)
                .sum(1);
```



**pv实现思路 2: process**

不同之处是在过滤之后调用process算子，也可以不过滤，在process中用条件语句进行过滤。

**需要注意的是，如果没有使用keyBy算子，需要设置程序的并行度为 1，确保行为为pv的所有数据进入同一个子任务。**

```java
   .keyBy(UserBehavior::getBehavior)
          .process(new KeyedProcessFunction<String, UserBehavior, Long>() {
              long count = 0;

              @Override
              public void processElement(UserBehavior value, Context ctx, Collector<Long> out) throws Exception {
                  count++;
                  out.collect(count);
              }
```

or

```java
               readTextFile("input/UserBehavior.csv")
                .map(line -> {
                    String[] datas = line.split(",");
                    return new UserBehavior(
                            Long.valueOf(datas[0]),
                            Long.valueOf(datas[1]),
                            Integer.valueOf(datas[2]),
                            datas[3],
                            Long.valueOf(datas[4]));
                })
                .process(new ProcessFunction<UserBehavior, Long>() {
                    Long sum = 0L;
                    @Override
                    public void processElement(UserBehavior value, Context ctx, Collector<Long> out) throws Exception {
                        if ("pv".equals(value.getBehavior())){
                            sum++;
                            out.collect(sum);
```



### 6.1.2   网站独立访客数（UV）的统计  

上一个案例中，我们统计的是所有用户对页面的所有浏览行为，也就是说，同一用户的浏览行为会被重复统计。而在实际应用中，我们往往还会关注，到底有多少不同的用户访问了网站，所以另外一个统计流量的重要指标是网站的独立访客数（Unique Visitor，UV）

实现思路：在PV的基础上对每个数据的userId进行去重，实现方法是将其存入一个hash-set，依据hash-set的大小来确定用户数。

```java
 .process(new KeyedProcessFunction<String, Tuple2<String, Long>, Integer>() {
              HashSet<Long> userIds = new HashSet<>();
              @Override
              public void processElement(Tuple2<String, Long> value, Context ctx, Collector<Integer> out) throws Exception {
                  userIds.add(value.f1);
                  out.collect(userIds.size());
              }
```

## 6.2   市场营销商业指标统计分析  

对于电商企业来说，一般会通过各种不同的渠道对自己的APP进行市场推广，而这些渠道的统计数据（比如，不同网站上广告链接的点击量、APP下载量）就成了市场营销的重要商业指标。

### 6.2.1   APP市场推广统计 - 分渠道  

封装数据的Java Bean类：

```java
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
//Lombok 是一种 Java™ 实用工具，可用来帮助开发人员消除 Java 的冗长，尤其是对于简单的 Java 对象（POJO）。它通过注解实现这一目的。
@Data
@AllArgsConstructor
@NoArgsConstructor
public class MarketingUserBehavior {
    private Long userId;
    private String behavior;
    private String channel;
    private Long timestamp;
}
```

具体实现代码:

```java
//创建上下文
//添加source
env.
    addSource(new AppMarketingDataSource())
    .map(behavior ->Tuple2.of(behavior.getChannel()+"_"+behavior.getBehavior(),1L))
    .keyBy(t->t.f0)
    .sum(1)
    .print();
env.execute();
public static class AppMarketingDataSource extends RichSourceFunction<MarketingUserBehavior> {
        boolean canRun = true;
        Random random = new Random();
        List<String> channels = Arrays.asList("huawwei", "xiaomi", "apple", "baidu", "qq", "oppo", "vivo");
        List<String> behaviors = Arrays.asList("download", "install", "update", "uninstall");

        @Override
        public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {
            while (canRun) {
                MarketingUserBehavior marketingUserBehavior = new MarketingUserBehavior(
                  (long) random.nextInt(1000000),
                  behaviors.get(random.nextInt(behaviors.size())),
                  channels.get(random.nextInt(channels.size())),
                  System.currentTimeMillis());
                ctx.collect(marketingUserBehavior);
                Thread.sleep(2000);
            }
        }

        @Override
        public void cancel() {
            canRun = false;
        }

```

### 6.2.2   APP市场推广统计 - 不分渠道  

```java
env
  .addSource(new AppMarketingDataSource())
  .map(behavior -> Tuple2.of(behavior.getBehavior(), 1L))
  .returns(Types.TUPLE(Types.STRING, Types.LONG))
  .keyBy(t -> t.f0)
  .sum(1)
  .print();
```

## 6.3 各省份页面广告点击量实时统计

更加具体的应用是，我们可以根据用户的地理位置进行划分，从而总结出不同省份用户对不同广告的偏好，这样更有助于广告的精准投放。

封装数据的Java Bean:

```java
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class AdsClickLog {
    private Long userId;
    private Long adId;
    private String province;
    private String city;
    private Long timestamp;
}
```

  具体实现代码 :

```java
env
          .readTextFile("input/AdClickLog.csv")
          .map(line -> {
              String[] datas = line.split(",");
              return new AdsClickLog(
                Long.valueOf(datas[0]),
                Long.valueOf(datas[1]),
                datas[2],
                datas[3],
                Long.valueOf(datas[4]));
          })
          .map(log -> Tuple2.of(Tuple2.of(log.getProvince(), log.getAdId()), 1L))
          .returns(TUPLE(TUPLE(STRING, LONG), LONG))
          .keyBy(new KeySelector<Tuple2<Tuple2<String, Long>, Long>, Tuple2<String, Long>>() {
              @Override
              public Tuple2<String, Long> getKey(Tuple2<Tuple2<String, Long>, Long> value) throws Exception {
                  return value.f0;
              }
          })
          .sum(1)
          .print("省份-广告");

        env.execute();
```

## 6.4   订单支付实时监控  

为了正确控制业务流程，也为了增加用户的支付意愿，网站一般会设置一个支付失效时间，超过一段时间不支付的订单就会被取消。另外，对于订单的支付，我们还应保证用户支付的正确性，这可以通过第三方支付平台的交易数据来做一个实时对账。

**需求:** **来自两条流的订单交易匹配**

​	对于订单支付事件，用户支付完成其实并不算完，我们还得确认平台账户上是否到账了。而往往这会来自不同的日志信息，所以我们要同时读入两条流的数据来做合并处理。

  Java  Bean  类的准备 :

```java
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
@Data
@AllArgsConstructor
@NoArgsConstructor
public class OrderEvent {
    private Long orderId;
    private String eventType;
    private String txId;
    private Long eventTime;
}
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TxEvent {
    private String txId;
    private String payChannel;
    private Long eventTime;
}
```

  具体实现:

分别读取两个流： SingleOutputStreamOperator

```java
        // 1. 读取Order流
        SingleOutputStreamOperator<OrderEvent> orderEventDS = env
          .readTextFile("input/OrderLog.csv")
          .map(line -> {
              String[] datas = line.split(",");
              return new OrderEvent(
                Long.valueOf(datas[0]),
                datas[1],
                datas[2],
                Long.valueOf(datas[3]));

          });
        // 2. 读取交易流
        SingleOutputStreamOperator<TxEvent> txDS = env
          .readTextFile("input/ReceiptLog.csv")
          .map(line -> {
              String[] datas = line.split(",");
              return new TxEvent(datas[0], datas[1], Long.valueOf(datas[2]));
          });
```

将两个流连接：

```java
 // 3. 两个流连接在一起
ConnectedStreams<OrderEvent, TxEvent> orderAndTx = orderEventDS.connect(txDS);
 // 4. 因为不同的数据流到达的先后顺序不一致，所以需要匹配对账信息.  输出表示对账成功与否
orderAndTx
.keyBy("txId", "txId")
    .process(new CoProcessFunction<OrderEvent, TxEvent, String>() {
              // 存 txId -> OrderEvent
              Map<String, OrderEvent> orderMap = new HashMap<>();
              // 存储 txId -> TxEvent
              Map<String, TxEvent> txMap = new HashMap<>();
              @Override
              public void processElement1(OrderEvent value, Context ctx, Collector<String> out) throws Exception {
                  // 获取交易信息
                  if (txMap.containsKey(value.getTxId())) {
                      out.collect("订单: " + value + " 对账成功");
                      txMap.remove(value.getTxId());
                  } else {
                      orderMap.put(value.getTxId(), value);
                  }
              }
                      @Override
              public void processElement2(TxEvent value, Context ctx, Collector<String> out) throws Exception {
                  // 获取订单信息
                  if (orderMap.containsKey(value.getTxId())) {
                      OrderEvent orderEvent = orderMap.get(value.getTxId());
                      out.collect("订单: " + orderEvent + " 对账成功");
                      orderMap.remove(value.getTxId());
                  } else {
                      txMap.put(value.getTxId(), value);
                  }
              }
          })
          .print();
        env.execute();
    })
```



# 7. Flink流处理高阶编程

所谓的高阶部分内容，其实就是Flink与其他计算框架不相同且占优势的地方，比如Window和Exactly-Once。

## 7.1   Flink的window机制  

### 7.1.1   窗口概述  

​	在流处理中，数据是连续不断的，因此不可能等到所有数据到了才开始处理。可以每来一个消息就处理一次，但是有时需要做一些聚合处理，例如：在过去的1分钟内有多少用户点击了我们的网页。在这种情况下，必须定义一个窗口，用来收集最近一分钟内的数据，并对这个窗口内的数据进行计算。

​	流式计算是一种被设计用于**处理无限数据集**的数据处理引擎，而无限数据集是指一种不断增长的本质上无限的数据集，而Window窗口是一种切割无限数据为有限块进行处理的手段。

​	在Flink中, 窗口(window)是处理无界流的核心. 窗口把流切割成有限大小的多个"存储桶"(bucket), 在这些桶上进行计算。

### 7.1.2 **窗口的分类**

  窗口分为 2  类:   

1. 基于时间的窗口(时间驱动)

2. 基于元素个数的(数据驱动)

####   7.1.2.1   基于时间的窗口 

时间窗口包含一个开始时间戳(包括)和结束时间戳(不包括), 这两个时间戳一起限制了窗口的尺寸.

​	在代码中, Flink使用Time Window这个类来表示基于时间的窗口.  提供了key查询开始时间戳和结束时间戳的方法, 还提供了针对给定的窗口获取它允许的最大时间差的方法(maxTimestamp())

​	 时间窗口又分 4 种: 

#####   滚动窗口( Tumbling  )  

​	滚动窗口固定大小, 不会重叠也没有缝隙。如果指定一个长度为5分钟的滚动窗口, 当前窗口开始计算, 每5分钟启动一个新的窗口.

​	滚动窗口能将数据流切分成不重叠的窗口，**每一个事件只能属于一个窗口**。

```java
.window(TumblingProcessingTimeWindows.of(Time.seconds(8))) // 添加滚动窗口
```

说明:

1. 时间间隔可以通过: 

   ```java
   Time.milliseconds(x)
   Time.seconds(x)
   Time.minutes(x)
   ```

   等等来指定。

2. 我们传递给window函数的对象叫**窗口分配器**。

#####   滑动窗口(  Sliding )  

​	与滚动窗口一样, 滑动窗口也有固定长度. 另外一个参数叫滑动步长, 用来控制滑动窗口启动的频率.

​	所以, 如果滑动步长小于窗口长度, 滑动窗口会重叠. 这种情况下, 一个元素可能会被分配到多个窗口中

```java
.window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5))) // 添加滚动窗
```

#####   会话窗口(  Session   )  

​	会话窗口分配器会根据活动元素进行分组. 不会有重叠, 与滚动窗口和滑动窗口相比, 会话窗口没有固定的开启和关闭时间.

​	如果会话窗口有一段时间没有收到数据, 会话窗口会自动关闭, 这段没有收到数据的时间就是会话窗口的gap(间隔)

​	我们可以配置静态的gap, 也可以通过一个gap extractor 函数来定义gap的长度. 当时间超过了这个gap, 当前的会话窗口就会关闭, 后序的元素会被分配到一个新的会话窗口

**示例代码:** 

1. 静态gap

```java
.window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
```



2. 动态gap

```java
.window(ProcessingTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor<Tuple2<String, Long>>() {
    @Override
    public long extract(Tuple2<String, Long> element) { // 返回 gap值, 单位毫秒
        return element.f0.length() * 1000;//依据数据流的元素的第一个值的长度来返回gap
    }
}))
```

 创建原理:  

​	因为会话窗口没有固定的开启和关闭时间, 所以会话窗口的创建和关闭与滚动,滑动窗口不同. 在Flink内部, 每到达一个新的元素都会创建一个新的会话窗口, 如果这些窗口彼此相距比较定义的gap小, 则会对他们进行合并. 为了能够合并, 会话窗口算子需要合并触发器和合并窗口函数: Reduce Function, Aggregate Function, or ProcessWindowFunction 

##### 全局窗口(  Global   )  

​	全局窗口分配器会分配相同key的所有元素进入同一个 Global window. 这种窗口机制只有**指定自定义的触发器**时才有用. 否则, 不会做任务计算, 因为这种窗口没有能够处理聚集在一起元素的结束点.

  示例代码:

```java
.window(GlobalWindows.create());
```

#### **7.1.2.1**   基于元素个数的窗口  

  按照指定的数据条数生成一个Window，与时间无关

**分2类:** 

#####  滚动窗口 

​	默认的Count Window是一个滚动窗口，只需要指定窗口大小即可，当元素数量达到窗口大小时，就会触发窗口的执行。

实例代码

```java
.countWindow(3)
```

​	说明:哪个窗口先达到3个元素, 哪个窗口就关闭. 不影响其他的窗口.

##### 滑动窗口

​	滑动窗口和滚动窗口的函数名是完全一致的，只是在传参数时需要传入两个参数，一个是window_size，一个是sliding_size。下面代码中的sliding_size设置为了2，也就是说，每收到两个相同key的数据就计算一次，每一次计算的window范围**最多**是3个元素。

 实例代码 

```java
.countWindow(3, 2)
```

### 7.1.3  Window   Function 

​	前面指定了窗口的分配器, 接着需要指定如何计算, 由*window function*来负责. 一旦窗口关闭,  *window function* 去计算处理窗口中的每个元素.

​	*window function* 可以是Reduce Function,

​	Aggregate Function,

​	or ProcessWindowFunction

​	中的任意一种.

​	Reduce Function,Aggregate Function更加高效, 原因就是Flink可以对到来的元素进行**增量聚合** **.** 		

​	ProcessWindowFunction 可以得到一个包含这个窗口中所有元素的迭代器, 以及元素所属窗口的元数据信息.

​	ProcessWindowFunction不能被高效执行的原因是Flink在执行这个函数之前, 需要在**内部缓存这个窗口上所有的元素**。

Reduce Function(增量聚合函数)

```java
.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
    .reduce(new ReduceFunction<Tuple2<String, Long> value1, Tuple2<String, Long> value2) throw Exception{
    sout(value1 + " " + value2);
     // value1是上次聚合的结果. 所以遇到每个窗口的第一个元素时, 这个函数不会进来
    return Tuple2.of(value1.f0, value1.f1 + value2.f1);
}
```

Aggregate Function(增量聚合函数)

```java
.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
    .aggregate(new AggregateFunction<Tuple2<String, Long>, Long, Long>(){
	    // 创建累加器: 初始化中间值
    @Override
        public Long createAccumulator() {
            sout("createAccumulator");
            return 0L;
        }
           // 累加器操作
    @Override
    public Long add(Tuple2<String, Long> value, Long accumulator) {
        System.out.println("add");
        return accumulator + value.f1;
    }

    // 获取结果
    @Override
    public Long getResult(Long accumulator) {
        System.out.println("getResult");
        return accumulator;
    }

    // 累加器的合并: 只有会话窗口才会调用
    @Override
    public Long merge(Long a, Long b) {
        System.out.println("merge");
        return a + b;
    }
    })
```

ProcessWindowFunction(全窗口函数)

```java
.process(new ProcessWindowFunction<Tuple2<String, Long>, Tuple2<String, Long>, String, TimeWindow>() {
    // 参数1: key 参数2: 上下文对象 参数3: 这个窗口内所有的元素 参数4: 收集器, 用于向下游传递数据
    @Override
    public void process(String key,
                        Context context,
                        Iterable<Tuple2<String, Long>> elements,
                        Collector<Tuple2<String, Long>> out) throws Exception {
        sout(context.window().getStart());
        long sum = 0L;
        for(Tuple2<String, Long> t : elements) {
        sum += t.f1;
        }
        out.collect(Tuple2.of(key, sum));}}}
```

## 7.2   Keyed vs Non-Keyed Windows  

​	在用window前首先需要确认应该是在key By后的流上用, 还是在没有key By的流上使用.

​	在keyed streams上使用窗口, 窗口计算被并行的运用在多个task上, 可以认为每个task都有自己单独窗口. 正如前面的代码所示.

​	在non-keyed stream上使用窗口, 流的并行度只能是1, 所有的窗口逻辑只能在一个单独的task上执行.

​	需要注意的是: 非key分区的流, 即使把并行度设置为大于1 的数, 窗口也只能在某个分区上使用.

### 7.3 Flink中的时间语义与Water Mark

#### 7.3.1 Flink中的时间语义

​	在Flink的流式操作中, 会涉及不同的时间概念.

##### 7.3.1.1 处理时间(process time)

​	处理时间是指的执行操作的各个设备的时间
​	对于运行在处理时间上的流程序, 所有的基于时间的操作(比如时间窗口)都是使用的设备时钟.比如, 一个长度为1个小时的窗口将会包含设备时钟表示的1个小时内所有的数据.  假设应用程序在 9:15am分启动, 第1个小时窗口将会包含9:15am到10:00am所有的数据, 然后下个窗口是10:00am-11:00am, 等等
​	处理时间是最简单时间语义, 数据流和设备之间不需要做任何的协调. 他提供了最好的性能和最低的延迟. 但是, 在分布式和异步的环境下, 处理时间没有办法保证确定性, 容易受到数据传递速度的影响: 事件的延迟和乱序
​	在使用窗口的时候, 如果使用处理时间, 就指定时间分配器为处理时间分配器

##### 7.3.1.2事件时间(event time)

​	事件时间是指的这个事件发生的时间.
​	在event进入Flink之前, 通常被嵌入到了event中, 一般作为这个event的时间戳存在.
​	在事件时间体系中, 时间的进度依赖于数据本身, 和任何设备的时间无关.  事件时间程序必须制定如何产生Event Time Watermarks(水印) . **在事件时间体系中, 水印是表示时间进度的标志(作用就相当于现实时间的时钟).**
​	在理想情况下，不管事件时间何时到达或者他们的到达的顺序如何, 事件时间处理将产生完全一致且确定的结果. 事件时间处理会在等待无序事件(迟到事件)时产生一定的延迟。由于只能等待有限的时间，因此这限制了确定性事件时间应用程序的可使用性。

​	在使用窗口的时候, 如果使用事件时间, 就指定时间分配器为事件时间分配器
注意: 
​	**在1.12之前默认的时间语义是处理时间, 从1.12开始, Flink内部已经把默认的语义改成了事件时间**

#### 7.3.2哪种时间更重要

#### 7.3.3 Flink中的 Water Mark

​	支持event time的流式处理框架需要一种能够测量event time 进度的方式.
​	**在Flink中去测量事件时间的进度的机制就是watermark(水印).** watermark作为数据流的一部分在流动, 并且携带一个时间戳t. 
​	一个Watermark(t)表示在这个流里面事件时间已经到了时间t, 意味着此时, 流中不应该存在这样的数据: 他的时间戳t2<=t (时间比较旧或者等于时间戳)

**有序流中的水印**
	当事件是有序的(按照他们自己的时间戳来看), watermark是流中一个简单的周期性的标记。

 **乱序流中的水印**
	当事件是乱序的, 则watermark对于这些乱序的流来说至关重要.
	通常情况下, 水印是一种标记, 是流中的一个点, 所有在这个时间戳(水印中的时间戳)前的数据应该已经全部到达. 一旦水印到达了算子, 则这个算子会提高他内部的时钟的值为这个水印的值.

#### 7.3.4 Flink中如何产生水印

​	在 Flink 中， 水印由应用程序开发人员生成。完美的水印永远不会错：时间戳小于水印标记时间的事件不会再出现。在特殊情况下（例如非乱序事件流），最近一次事件的时间戳就可能是完美的水印。
​	启发式水印则相反，它只估计时间，因此有可能出错， 即迟到的事件 （其时间戳小于水印标记时间）晚于水印出现。针对启发式水印， Flink 提供了处理迟到元素的机制。
​	设定水印通常需要用到领域知识。举例来说，如果知道事件的迟到时间不会超过 5 秒， 就可以将水印标记时间设为收到的最大时间戳减去 5 秒。 另 一种做法是，采用一个 Flink 作业监控事件流，学习事件的迟到规律，并以此构建水印生成模型。

#### 7.3.5 Event Time和Water Mark的使用

Flink内置了两个Water Mark生成器:

1. Monotonously Increasing Timestamps(时间戳单调增长:其实就是允许的延迟为0)

```java
WatermarkStrategy.forMonotonousTimestamps();
```

2. Fixed Amount of Lateness(允许固定时间的延迟)

```java
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> stream = env
          .socketTextStream("hadoop102", 9999)  // 在socket终端只输入毫秒级别的时间戳
            .map(new MapFunction<String, WaterSensor>(){
              @Override
              public WaterSensor map(String value) throws Exception {
                   String[] datas = value.split(",");
                  return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
              }
          });
        // 创建水印生产策略
        WatermarkStrategy<WaterSensor> wms = WatermarkStrategy
            .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))// 最大容忍的延迟时间
             @Override 
              public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                  return element.getTs() * 1000;
              }
          });
            stream
          .assignTimestampsAndWatermarks(wms) // 指定水印和时间戳
          .keyBy(WaterSensor: :getId)
          .window(TumblingEventTimeWindows.of(Time.seconds(5)))
          .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
              @Override
              public void process(String key, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                  String msg = "当前key: " + key
                    + "窗口: [" + context.window().getStart() / 1000 + "," + context.window().getEnd()/1000 + ") 一共有 "
                    + elements.spliterator().estimateSize() + "条数据 ";
                  out.collect(msg);
              }
          })
          .print();
        env.execute();
```

#### 7.3.6自定义Watermark Strategy

​	有2种风格的Water Mark生产方式: periodic(周期性) and punctuated(间歇性).都需要继承接口: Watermark Generator

周期性:

```java
       // 创建水印生产策略
        WatermarkStrategy<WaterSensor> myWms = new WatermarkStrategy<WaterSensor>() {
            @Override
            public WatermarkGenerator<WaterSensor> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                System.out.println("createWatermarkGenerator ....");
                return new MyPeriod(3);
            }
        }.withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
            @Override
            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                System.out.println("recordTimestamp  " + recordTimestamp);
                return element.getTs() * 1000;

```

间歇性:



### 7.8 Flink状态编程

有状态的计算是流处理框架要实现的重要功能，因为稍复杂的流处理场景都需要记录状态，然后在新流入数据的基础上不断更新状态。

Spark Streaming在状态管理这块做的不好, 很多时候需要借助于外部存储(例如Redis)来手动管理状态, 增加了编程的难度。
Flink的状态管理是它的优势之一。

#### 7.8.1什么是状态

​	在流式计算中有些操作一次处理一个独立的事件(比如解析一个事件), 有些操作却需要记住多个事件的信息(比如窗口操作)。需要记住多个事件信息的操作就是有状态的。

流式计算分为无状态计算和有状态计算两种情况。

无状态的计算观察每个独立事件，并根据最后一个事件输出结果。例如，流处理应用程序从传感器接收水位数据，并在水位超过指定高度时发出警告。

有状态的计算则会基于多个事件输出结果。以下是一些例子。例如，计算过去一小时的平均水位，就是有状态的计算。所有用于复杂事件处理的状态机。例如，若在一分钟内收到两个相差20cm以上的水位差读数，则发出警告，这是有状态的计算。流与流之间的所有关联操作，以及流与静态表或动态表之间的关联操作，都是有状态的计算。

#### 7.8.2为什么需要管理状态

​	下面的几个场景都需要使用流处理的状态功能:
**去重**
​	数据流中的数据有重复，我们想对重复数据去重，需要记录哪些数据已经流入过应用，当新数据流入时，根据已流入过的数据来判断去重。

**检测**
	检查输入流是否符合某个特定的模式，需要将之前流入的元素以状态的形式缓存下来。比如，判断一个温度传感器数据流中的温度是否在持续上升。

**聚合**
	对一个时间窗口内的数据进行聚合分析，分析一个小时内水位的情况。

**更新机器学习模型**

​	在线机器学习场景下，需要根据新流入数据不断更新机器学习的模型参数。



### 7.8.3   Flink中的状态分类  

​	Flink包括两种基本类型的状态Managed State和Raw State

|                  | **Managed** **State**                                   | **Raw** **State** |
| ---------------- | ------------------------------------------------------- | ----------------- |
| **状态管理方式** | Flink Runtime托管, 自动存储, 自动恢复, 自动伸缩         | 用户自己管理      |
| **状态数据结构** | Flink提供多种常用数据结构, 例如:List State, Map State等 | 字节数组: byte[]  |
| **使用场景**     | 绝大数Flink算子                                         | 所有算子          |

**注意:**

1. 从具体使用场景来说，绝大多数的算子都可以通过继承Rich函数类或其他提供好的接口类，在里面使用Managed State。Raw State一般是在已有算子和Managed State不够用时，用户自定义算子时使用。

2. 在我们平时的使用中Managed State已经足够我们使用, 下面重点学习Managed State

### 7.8.4   Managed State  的分类  

​	对Managed State继续细分，它又有两种类型

a) Keyed State(键控状态)

b) Operator State(算子状态)

|                    | **Operator** **State**                                       | **Keyed** **State**                                          |
| ------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ |
| **适用用算子类型** | 可用于所有算子: 常用于source, 例如 FlinkKafkaConsumer        | 只适用于KeyedStream上的算子                                  |
| **状态分配**       | 一个算子的子任务对应一个状态                                 | 一个Key对应一个State: 一个算子会处理多个Key, 则访问相应的多个State |
| **创建和访问方式** | 实现CheckpointedFunction或ListCheckpointed(已经过时)接口     | 重写RichFunction, 通过里面的RuntimeContext访问               |
| **横向扩展**       | 并发改变时有多重重写分配方式可选: 均匀分配和合并后每个得到全量 | 并发改变, State随着Key在实例间迁移                           |
| **支持的数据结构** | List State和Broadcast State                                  | Value State, List State,Map State， Reduce State, Aggregating State |



### 7.8.5 算子状态的使用

​	Operator State可以用在所有算子上，每个算子子任务或者说每个算子实例共享一个状态，流入这个算子子任务的数据可以访问和更新这个状态。

​	**注意：算子子任务之间的状态不能互相访问**

​	应用场景：Source或者Sink等算子上，用来保存流入数据的偏移量或者对输出数据进行缓存，以达到Flink的Exactly-Once 语义。

### 7.8.6 算子状态的数据结构

​	列表状态（List state）

将状态表示为一组数据的列表

​	联合列表状态(Union list state)

和列表状态类似，区别在于发生故障时可以从save point启动程序进行恢复

一种是均匀分配的（List State）,一种是将所有state合并为全量state再分发给每个实例（Union list state）

​	广播状态(Broadcast state)

如果一个算子有多项任务，每一项的状态又都相同，这种情况适合广播状态。



**案例1: 列表状态**

在map算子中计算数据的个数：

```java
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink01_State_Operator {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
          .getExecutionEnvironment()
          .setParallelism(3);
        env
          .socketTextStream("hadoop102", 9999)
          .map(new MyCountMapper())
          .print();

        env.execute();
    }

    private static class MyCountMapper implements MapFunction<String, Long>, CheckpointedFunction {
        private Long count = 0L;
        private ListState<Long> state;

        @Override
        public Long map(String value) throws Exception {
            count++;
            return count;
        }

        // 初始化时会调用这个方法，向本地状态中填充数据. 每个子任务调用一次
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            System.out.println("initializeState...");
            state = context
              .getOperatorStateStore()
              .getListState(new ListStateDescriptor<Long>("state", Long.class));
            for (Long c : state.get()) {
                count += c;
            }
        }

        // Checkpoint时会调用这个方法，我们要实现具体的snapshot逻辑，比如将哪些本地状态持久化
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            System.out.println("snapshotState...");
            state.clear();
            state.add(count);
        }
```



**案例2: 广播状态**

从版本1.5.0开始，Apache Flink具有一种新的状态，称为**广播状态**。

广播状态与其他算子状态的区别是：

1. 它是一个map格式

2. 它只对输入有广播流和无广播流的特定算子可用

3. 这样的算子可以具有不同名称的多个广播状态。

```java
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
public class Flink01_State_Operator_3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
          .getExecutionEnvironment()
          .setParallelism(3);
        DataStreamSource<String> dataStream = env.socketTextStream("hadoop102", 9999);
        DataStreamSource<String> controlStream = env.socketTextStream("hadoop102", 8888);


        MapStateDescriptor<String, String> stateDescriptor = new MapStateDescriptor<>("state", String.class, String.class);
        // 广播流
        BroadcastStream<String> broadcastStream = controlStream.broadcast(stateDescriptor);
        dataStream
          .connect(broadcastStream)
          .process(new BroadcastProcessFunction<String, String, String>() {
              @Override
              public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                  // 从广播状态中取值, 不同的值做不同的业务
                  ReadOnlyBroadcastState<String, String> state = ctx.getBroadcastState(stateDescriptor);
                  if ("1".equals(state.get("switch"))) {
                      out.collect("切换到1号配置....");
                  } else if ("0".equals(state.get("switch"))) {
                      out.collect("切换到0号配置....");
                  } else {
                      out.collect("切换到其他配置....");
                  }
              }

              @Override
              public void processBroadcastElement(String value, Context ctx, Collector<String> out) throws Exception {
                  BroadcastState<String, String> state = ctx.getBroadcastState(stateDescriptor);
                  // 把值放入广播状态
                  state.put("switch", value);
              }
          })
          .print();

        env.execute();
```



### 7.8.7 键控状态的使用

键控状态是根据输入数据流中定义的key来维护和访问的。

每一个key维护一个状态实例，相同key的数据分到同一个子任务中，这个子任务会维护和处理对应的状态。

**因此，相同key的所有元素都会访问相同的状态。**

**数据结构：**

类似于一个分布式的key-value map结构，只能用于keyed stream（keyBy算子处理之后的）

**键控状态支持的数据类型**：

  **ValueState<T>**
保存单个值. 每个有key有一个状态值.  设置使用 update(T), 获取使用 T value()

  **ListState<T>:**
保存元素列表.  添加元素: add(T)  addAll(List<T>)
获取元素: Iterable<T> get()
覆盖所有元素: update(List<T>)

  **ReducingState<T>:** 
存储单个值, 表示把所有元素的聚合结果添加到状态中.  与ListState类似, 但是当使用add(T)的时候ReducingState会使用指定的ReduceFunction进行聚合.

  **AggregatingState<IN, OUT>:** 
存储单个值. 与ReducingState类似, 都是进行聚合. 不同的是, AggregatingState的聚合的结果和元素类型可以不一样.

  **MapState<UK, UV>:** 
存储键值对列表. 
添加键值对:  put(UK, UV) or putAll(Map<UK, UV>)
根据key获取值: get(UK)
获取所有: entries(), keys() and values()
检测是否为空: isEmpty()

**案例1:ValueState**

流程：获取上下文----->设置并行度----->获取流----->map操作元素，封装成POJO----->keyBy操作，让相同的传感器数据流入同一个子任务----->process操作，其中传入KeyedProcessFunction，重写open 和 processElement方法，拥有一个状态属性：对应state----->其中open是根据运行时上下文获取对应类型的键控状态，其中调用get对应State方法时要传入对应StateDescriptor----->processElement方法是对获取的元素的状态进行对应操作：对应state.add/对应state.update 方法

```java
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
public class Flink02_State_Keyed_Value {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
          .getExecutionEnvironment()
          .setParallelism(3);
        env
          .socketTextStream("hadoop102", 9999)
          .map(value -> {
              String[] datas = value.split(",");
              return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));

          })
          .keyBy(WaterSensor::getId)
          .process(new KeyedProcessFunction<String, WaterSensor, String>() {
              private ValueState<Integer> state;
              @Override
              public void open(Configuration parameters) throws Exception {
                  state = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("state", Integer.class));
              }

              @Override
              public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                  Integer lastVc = state.value() == null ? 0 : state.value();
                  if (Math.abs(value.getVc() - lastVc) >= 10) {
                      out.collect(value.getId() + " 红色警报!!!");
                  }
                  state.update(value.getVc());
              }
          })
          .print();
        env.execute();

```



**案例2:ListState**

针对每个传感器输出最高的3个水位值

流程：获取上下文----->设置并行度----->获取流----->map操作元素，封装成POJO----->keyBy操作，让相同的传感器数据流入同一个子任务----->process操作，其中传入KeyedProcessFunction，重写open 和 processElement方法，拥有一个状态属性：对应state----->其中open是根据运行时上下文获取对应类型的键控状态，其中调用get对应State方法时要传入对应StateDescriptor----->processElement方法是对获取的元素的状态进行对应操作：对应state.add/对应state.update 方法

```java
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
public class Flink02_State_Keyed_ListState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
          .getExecutionEnvironment()
          .setParallelism(3);
        env
          .socketTextStream("hadoop102", 9999)
          .map(value -> {
              String[] datas = value.split(",");
              return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));

          })
          .keyBy(WaterSensor::getId)
          .process(new KeyedProcessFunction<String, WaterSensor, List<Integer>>() {
              private ListState<Integer> vcState;

              @Override
              public void open(Configuration parameters) throws Exception {
                  vcState = getRuntimeContext().getListState(new ListStateDescriptor<Integer>("vcState", Integer.class));
              }

              @Override
              public void processElement(WaterSensor value, Context ctx, Collector<List<Integer>> out) throws Exception {
                  vcState.add(value.getVc());
                  //1. 获取状态中所有水位高度, 并排序
                  List<Integer> vcs = new ArrayList<>();
                  for (Integer vc : vcState.get()) {
                      vcs.add(vc);
                  }
                  // 2. 降序排列
                  vcs.sort((o1, o2) -> o2 - o1);
                  // 3. 当长度超过3的时候移除最后一个
                  if (vcs.size() > 3) {
                      vcs.remove(3);
                  }
                  vcState.update(vcs);
                  out.collect(vcs);
              }
          })
          .print();
        env.execute();
```

**案例3:ReducingState**

计算每个传感器的水位和

流程：获取上下文----->设置并行度----->获取流----->map操作元素，封装成POJO----->keyBy操作，让相同的传感器数据流入同一个子任务----->process操作，其中传入KeyedProcessFunction，重写open 和 processElement方法，拥有一个状态属性：对应state----->其中open是根据运行时上下文获取对应类型的键控状态，其中调用get对应State方法时要传入对应StateDescriptor----->processElement方法是对获取的元素的状态进行对应操作：对应state.add/对应state.update 方法

```java
.process(new KeyedProcessFunction<String, WaterSensor, Integer>() {
    private ReducingState<Integer> sumVcState;
    @Override
    public void open(Configuration parameters) throws Exception {
        sumVcState = this
          .getRuntimeContext()
          .getReducingState(new ReducingStateDescriptor<Integer>("sumVcState", Integer::sum, Integer.class));
    }

    @Override
    public void processElement(WaterSensor value, Context ctx, Collector<Integer> out) throws Exception {
        sumVcState.add(value.getVc());
        out.collect(sumVcState.get());

```

**案例4:AggregatingState**
计算每个传感器的平均水位

```java
.process(new KeyedProcessFunction<String, WaterSensor, Double>() {

    private AggregatingState<Integer, Double> avgState;

    @Override
    public void open(Configuration parameters) throws Exception {
        AggregatingStateDescriptor<Integer, Tuple2<Integer, Integer>, Double> aggregatingStateDescriptor = new AggregatingStateDescriptor<>("avgState", new AggregateFunction<Integer, Tuple2<Integer, Integer>, Double>() {
            @Override
            public Tuple2<Integer, Integer> createAccumulator() {
                return Tuple2.of(0, 0);
            }

            @Override
            public Tuple2<Integer, Integer> add(Integer value, Tuple2<Integer, Integer> accumulator) {
                return Tuple2.of(accumulator.f0 + value, accumulator.f1 + 1);
            }

            @Override
            public Double getResult(Tuple2<Integer, Integer> accumulator) {
                return accumulator.f0 * 1D / accumulator.f1;
            }

            @Override
            public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
                return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
            }
        }, Types.TUPLE(Types.INT, Types.INT));
        avgState = getRuntimeContext().getAggregatingState(aggregatingStateDescriptor);
    }

    @Override
    public void processElement(WaterSensor value, Context ctx, Collector<Double> out) throws Exception {
        avgState.add(value.getVc());
        out.collect(avgState.get());
```

**案例5:MapState**

去重: 去掉重复的水位值. 思路: 把水位值作为MapState的key来实现去重, value随意

```java
.process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
    private MapState<Integer, String> mapState;
    @Override
    public void open(Configuration parameters) throws Exception {
        mapState = this
          .getRuntimeContext()
          .getMapState(new MapStateDescriptor<Integer, String>("mapState", Integer.class, String.class));
    }
    @Override
    public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
        if (!mapState.contains(value.getVc())) {
            out.collect(value);
            mapState.put(value.getVc(), "随意");
```



### 7.8.8状态后端

每次传入一条数据，有状态的算子任务都会读取和更新状态。由于有效的访问状态对于处理数据的低延迟很重要，因此每个并行任务，子任务都会在本地维护状态来确保快速的状态访问。

状态的访问和存储以及维护由一个可插入的组件决定，叫做状态后端（state backend）

状态后端的主要任务：

本地状态的管理（Taskmanager）

检查点（checkpoint）状态写入远程存储

 **状态后端的分类** 

#### **7.8.8.1 MemoryStateBackend**

内存级别的状态后端(默认), 

存储方式:本地状态存储在TaskManager的内存中, checkpoint 存储在JobManager的内存中.
特点:快速, 低延迟, 但不稳定

使用场景: 1. 本地测试 2. 几乎无状态的作业(ETL) 3. JobManager不容易挂, 或者挂了影响不大. 4. 不推荐在生产环境下使用

#### **7.8.8.2 FsStateBackend**

存储方式: 本地状态在TaskManager内存, Checkpoint时, 存储在文件系统(hdfs)中
特点: 拥有内存级别的本地访问速度, 和更好的容错保证

使用场景: 1. 常规使用状态的作业. 例如分钟级别窗口聚合, join等 2. 需要开启HA的作业 3. 可以应用在生产环境中

#### **7.8.8.3 RocksDBStateBackend**

将所有的状态序列化之后, 存入本地的RocksDB数据库中.(一种No Sql数据库, KV形式存储)

存储方式: 1. 本地状态存储在TaskManager的RocksDB数据库中(实际是内存+磁盘) 2. Checkpoint在外部文件系统(hdfs)中.

使用场景: 1. 超大状态的作业, 例如天级的窗口聚合 2. 需要开启HA的作业 3. 对读写状态性能要求不高的作业 4. 可以使用在生产环境



#### **配置状态后端**

##### **全局配置状态后端**

在flink-conf.yaml文件中设置默认的全局后端

##### **在代码中配置状态后端**

可以在代码中单独为这个Job设置状态后端.

```java
env.setStateBackend(new MemoryStateBackend());
env.setStateBackend(new FsStateBackend("hdfs://hadoop162:8020/flink/checkpoints/fs"));
```

如果要使用RocksDBBackend, 需要先引入依赖:

```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-statebackend-rocksdb_${scala.binary.version}</artifactId>
    <version>${flink.version}</version>
</dependency>
```

```java
env.setStateBackend(new RocksDBStateBackend("hdfs://hadoop162:8020/flink/checkpoints/rocksdb"));
```

### 7.9 Flink的容错机制

#### 7.9.1 状态的一致性

Flink的一个重大价值在于，它**既**保证了**exactly-once**，**又**具有**低延迟和高吞吐**的处理能力。

#### 端到端**的状态一致性**

每一个组件都需要保证一致性，端到端的一致性级别取决于所有组件中最弱的组件。

具体划分如下：

 **source端**

需要外部源可重设数据的读取位置.目前我们使用的Kafka Source具有这种特性: 读取数据的时候可以指定offset

**flink内部**
依赖checkpoint机制

**sink端**
需要保证从故障恢复时，数据不会重复写入外部系统. 有2种实现形式:

**a)幂等（Idempotent）写入**
所谓幂等操作，是说一个操作，可以重复执行很多次，但只导致一次结果更改，也就是说，后面再重复执行就不起作用了。

**b)事务性（Transactional）写入**

需要构建事务来写入外部系统，构建的事务对应着 checkpoint，等到 checkpoint 真正完成的时候，才把所有对应的结果写入 sink 系统中。

对于事务性写入，具体又有两种实现方式：**预写日志（WAL）**和**两阶段提交（2PC）**

#### 7.9.2 Checkpoint原理

Flink具体如何保证exactly-once呢? 它使用一种被称为"检查点"（checkpoint）的特性，在出现故障时将系统重置回正确状态。下面通过简单的类比来解释检查点的作用。
假设你和两位朋友正在数项链上有多少颗珠子，如下图所示。你捏住珠子，边数边拨，每拨过一颗珠子就给总数加一。你的朋友也这样数他们手中的珠子。当你分神忘记数到哪里时，怎么办呢? 如果项链上有很多珠子，你显然不想从头再数一遍，尤其是当三人的速度不一样却又试图合作的时候，更是如此(比如想记录前一分钟三人一共数了多少颗珠子，回想一下一分钟滚动窗口)。

# 8.Flink流处理高阶编程实战

# 9.Flink CEP编程

# 10.Flink CEP编程实战

