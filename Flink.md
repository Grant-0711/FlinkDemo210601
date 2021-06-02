# 1. Flink简介



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



# 4.Flink运行架构





## 5.行动算子

### 5.1 区别

行动算子用来提交Job！和转换算子不同的时，转换算子一般是懒执行的！转换算子需要行动算子触发！



### 5.2 常见的行动算子

| 算子         | 解释                                                         | 备注                                                   |
| ------------ | ------------------------------------------------------------ | ------------------------------------------------------ |
| reduce       | 将RDD的所有的元素使用一个函数进行归约，返回归约后的结果      |                                                        |
| collect      | 将RDD的所有元素使用Array进行返回，收集到Driver               | 慎用！如果RDD元素过多，Driver端可能会OOM！             |
| count        | 统计RDD中元素的个数                                          |                                                        |
| take(n:Int)  | 取前N个元素                                                  | 慎用！如果取RDD元素过多，Driver端可能会OOM！           |
| takeOrdered  | 取排序后的前N个元素                                          | 慎用！如果取RDD元素过多，Driver端可能会OOM！           |
| first        | 返回第一个元素                                               |                                                        |
| aggregate    | 和aggregateByKey算子类似，不同的是zeroValue在分区间聚合时也会参与运算 |                                                        |
| fold         | 简化版的aggregate，分区内和分区间的运算逻辑一样              |                                                        |
| countByValue | 统计相同元素的个数                                           |                                                        |
| countByKey   | 针对RDD[(K,V)]类型的RDD，统计相同key对应的K-V的个数          |                                                        |
| foreach      | 遍历集合中的每一个元素，对元素执行函数                       | 特殊场景（向数据库写入数据），应该使用foreachPartition |
| save相关     | 将RDD中的数据保存为指定格式的文件                            | 保存为SequnceFile文件，必须是RDD[(K,V)]                |
| 特殊情况     | new RangeParitioner时，也会触发Job的提交！                   |                                                        |
|              |                                                              |                                                        |

## 6.序列化

### 6.1 序列化

如果转换算子存在闭包，必须保证闭包可以被序列化（使用外部变量可以被序列化）！

否则报错 Task not Serilizable，Job不会被提交！



解决：  ①闭包使用的外部变量 extends Serializable

​				②使用case class声明外部变量



特殊情况：  如果闭包使用的外部变量是某个类的属性或方法，此时这个类也需要被序列化！

​					

​				解决：  ①类也需要被序列化

​							②在使用某个类的属性时，可以使用局部变量 接收  属性

​							③在使用某个类的方法时，可以使用函数（匿名函数也可以） 代替 方法

### 6.2 Kryo

Kryo是Spark中高效的序列化框架！

使用： SparkConf.registerKryoClasses(Array(xxx))



## 7.依赖和血缘

### 7.1 依赖

查看： rdd.dependencies 查看



依赖描述的是当前RDD和父RDD之间，分区的对应关系！



```scala
abstract class Dependency[T] extends Serializable {
  def rdd: RDD[T]
}
```

```
NarrowDependency: 窄依赖
	RangeDependency： 1子多父
	OneToOneDependency： 1子1父
	
ShuffleDependency：宽依赖
```



作用： Spark内部使用！Spark任务在提交时，DAG调度器需要根据提交的当前RDD的dependencies 信息

​					来获取当前rdd的依赖类型，根据依赖类型划分阶段！

​				如果是ShuffleDependency，就会产生一个新的阶段！



### 7.2 血缘关系

RDD.toDebugString 查看



作用： Spark有容错机制！一旦某个RDD计算失败，或数据丢失，重新计算！重新计算时，需要根据血缘关系，从头到尾依次计算！ 是Spark容错机制的保障！



## 8.持久化

### 8.1 cache

cache的作用： 加速查询效率，缓存多使用内存作为存储设备！

​							在spark中，缓存可以用来缓存已经计算好的RDD，避免相同RDD的重复计算！



使用：   rdd.cache()    等价于   rdd.persist()  等价于  rdd.persist(Storage.MEMORY_ONLY)

​				rdd.persist(缓存的级别)



缓存会在第一个Job提交时，触发，之后相同的Job如果会基于缓存的RDD进行计算，优先从缓存中读取RDD！

​			

缓存多使用内存作为存储设备，内存是有限的，因此缓存多有淘汰策略，在Spark中，默认使用LRU（less recent use）算法淘汰缓存中的数据！



缓存因为不可靠性，所以并不会影响血缘关系！

带有shuffle的算子，会自动将MapTask阶段溢写到磁盘的文件，一致保存。如果有相同的Job使用到了这个文件，此时这个Map阶段可以被跳过，可以理解为使用到了缓存！



### 8.2 checkpoint

checkpoint为了解决cache的不可靠性！设置了checkpoint目录后，Job可以将当前RDD的数据，或Job的状态持久化到文件系统中！



作用： ①从文件系统中读取已经缓存的RDD

​			②当Job失败，需要重试时，从文件系统中获取之前Job运行的状态，基于状态恢复运行



使用：  SparkContext.setCheckpointDir (如果是在集群中运行，必须使用HDFS上的路径)

​				rdd.checkpoint()



checkpoint会在第一个 Job提交时，额外提交一个Job进行计算，如果要避免重复计算，可以和cache结合使用！





## 9.共享变量

### 9.1 广播变量

​		解决在Job中，共享只读变量！

​		可以将一个Job多个stage共有的大体量的数据，使用广播变量，发送给每台机器（executor），这个executor上的task都可以共享这份数据！



使用： var br= SparkContext.broadcast(v)

​		   br.value() //访问值



使用广播变量，必须是只读变量，不能修改！



### 9.2累加器

​		解决计数（类似MR中的Counter）以及sum求和的场景，比reduceByKey之类的算子高效！并行累加，之后在Driver端合并累加的结果！



使用：  ①Spark默认提供了数值类型的累加器，用户也可以自定义累加器，通过继承AccumulatorV2[IN,OUT]

​				②如果是自定义的累加器，需要进行注册

​							SparkContext.regist(累加器，名称)

​				③使用

​								累加：  add()

​								获取累加的结果： value()		

​					自定义累加器，必修实现以下方法：

​								reset(): 重置归0

​								isZero():判断是否归0

​								merge(): 合并相同类型的累加器的值



运行过程：  在Driver端创建一个累加器，基于这个累加器，每个Task都会先调用

​						Copy----->reset----->isZero

​							如果isZero返回false，此时就报错，Job没有提交，返回true，此时

​					将累加器随着Task序列化！

​						在Driver端，调用value方法合并所有（包括在Driver创建的）的累加器



注意： 累加器为了保证累加的精确性，必须每次在copy()后，将累加器重置归0

​			在算子中，不能调用value,只能在Driver端调用

​			累加器的输入和输出可以是不一样的类型



## 10.读写数据

读写文本：

```
		读：  SparkContext.textFile()
		写： RDD.saveAsTextFile
```



读写Object文件：

```
读：SparkContext.ObjectFile[T]()
写： RDD.saveAsObjectFile
```

读写SF文件：

```
读：SparkContext.SequenceFile[T]()
写： RDD.saveAsSequenceFileFile

RDD必须是K-V类型
```



读写JDBC：

```
读： new JDBCRDD()
写：  RDD.foreachePartition()
```



读写HBase：

```
读：    TableInputFormat
				RR:  key: ImmutableBytesWritable
					 value:  Result
					 
	   new NewHadoopRDD
	   
写：     TableOutputFormat
				RW： key： 随意，一般使用ImmutableBytesWritable存储rowkey
					 value:  Mutation (Put,Delete,Append)
					 
		RDD必须是K-V类型
			RDD.saveAsNewApiHadoopDataSet
```

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

### 5.2.4  从Kafka读取数据

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

使用累加器实现！



#### 2.2.2 实现方式二



## 3.常用算子

```sql
select     // udf   一进一出    trim
	xxx,sum(),count()    //map
from xxx
where  xx       // filter
group by xx      // reduceByKey
having xxx      //filter
order by xxx    // sortby sortbyKey
limit xxx     // take 

            // join,leftOuterJoin
```



## 4.需求二



## 5.需求三

页面单跳： 由一个页面直接跳转到另一个页面

```
--单跳
A ----> B ------>C

-- 不符合
A-----> E ------>B
```

A--B的单跳： 1次



页面单跳转换率：  页面单跳次数 / 页面单跳中首页面的访问次数

上述案例：  页面单跳转换率 为 1 / 2 





步骤： ①求当前数据中所有的页面被访问的次数

​			 ②求每个页面单跳的次数

​						先求出页面的访问顺序

 						a) 求出每个用户，每个SESSION访问的记录，之后按照访问时间升序排序

```
a,session1,2020-11-11 11:11:11, A
a,session1,2020-11-11 11:12:11, B
a,session1,2020-11-11 11:11:09, C
a,session1,2020-11-11 11:13:11, A

a,session1, C---A---B---A

a,session2,2020-11-11 11:11:11, A
a,session2,2020-11-11 11:12:11, B 

a,session2, A----B
```

​						b)按照排序的结果求 页面的访问顺序,拆分页面单跳

```
a,session1, C---A---B---A
C---A
A---B
B---A

a,session2, A----B
A---B
```

​						c) 聚合所有的单跳记录，求出每个单跳的次数

```
C---A  1
A---B  2
B---A  1
```

​	     ③求转换率

```
A---3
B---2
C----1

----------
C---A  1
A---B  2
B---A  1

-----------
C---A  1/1
A---B  2/3
B---A  1/2
```



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

PV实现思路一：WordCount

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

pv实现思路 2: process

不同之处是在过滤之后调用process算子

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

#####   滚动窗口( Tumbling Windows  )  

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

#####   滑动窗口(  Sliding Windows  )  

​	与滚动窗口一样, 滑动窗口也有固定长度. 另外一个参数叫滑动步长, 用来控制滑动窗口启动的频率.

​	所以, 如果滑动步长小于窗口长度, 滑动窗口会重叠. 这种情况下, 一个元素可能会被分配到多个窗口中

```java
.window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5))) // 添加滚动窗
```

#####   会话窗口(  Session Windows  )  

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

##### 全局窗口(  Global Windows  )  

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



# 8.Flink流处理高阶编程实战

# 9.Flink CEP编程

# 10.Flink CEP编程实战

