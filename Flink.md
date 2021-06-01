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



### 2.3 测试

测试： 

spark-shell --master spark://hadoop103:7077:  交互式环境

```scala

```



spark-submit:  用来提交jar包

用法： spark-submit  [options]   jar包   jar包中类的参数

```shell

```



### 2.4 --deploy-mode

client(默认)：  会在Client本地启动Driver程序！

```

```

cluster:    会在集群中选择其中一台机器启动Driver程序！

```

```



区别：

①Driver程序运行位置的不同会影响结果的查看！

​				client： 将某些算子的结果收集到client端！

​								要求client端Driver不能中止！

​				cluster（生产）:  需要在Driver运行的executor上查看日志！



②client：  jar包只需要在client端有！

​	cluster:  保证jar包可以在集群的任意台worker都可以读到！



## 3.配置历史服务

作用：在Job离线期间，依然可以通过历史服务查看之前生成的日志！

①配置 SPARK_HOME/conf/spark-defaults.conf

影响启动的spark应用

```
spark.eventLog.enabled true
#需要手动创建
spark.eventLog.dir hdfs://hadoop102:9820/sparklogs
```



②配置 SPARK_HOME/conf/spark-env.sh

影响的是spark的历史服务进程

```
export SPARK_HISTORY_OPTS="
-Dspark.history.ui.port=18080 
-Dspark.history.fs.logDirectory=hdfs://hadoop102:9820/sparklogs
-Dspark.history.retainedApplications=30"
```



③分发以上文件，重启集群

④启动历史服务进程

```
sbin/start-historyserver.sh
```



## 4.spark on yarn

YARN： 集群！

Spark:  相对于YARN来说，就是一个客户端，提交Job到YARN上运行！

​			 

MR ON YARN提交流程：  ①Driver 请求 RM，申请一个applicatiion

​											 ②RM接受请求，此时，RM会首先准备一个Container运行 ApplicationMaster

​											 ③ ApplicationMaster启动完成后，向RM进行注册，向RM申请资源运行

​													其他任务

​												Hadoop MR :  申请Container运行MapTask，ReduceTask

​												Spark：   申请Container运行 Executor



只需要选择任意一台可以连接上YARN的机器，安装Spark即可！



### 4.1 配置

确认下YARN的 yarn-site.xml配置中

```xml
<!--是否启动一个线程检查每个任务正使用的物理内存量，如果任务超出分配值，则直接将其杀掉，默认是true -->
<property>
     <name>yarn.nodemanager.pmem-check-enabled</name>
     <value>false</value>
</property>

<!--是否启动一个线程检查每个任务正使用的虚拟内存量，如果任务超出分配值，则直接将其杀掉，默认是true -->
<property>
     <name>yarn.nodemanager.vmem-check-enabled</name>
     <value>false</value>
</property>

```

```xml
<property>
        <name>yarn.log.server.url</name>
        <value>http://hadoop102:19888/jobhistory/logs</value>
    </property>
```



分发 yarn-site.xml，重启YARN！



编辑 SPARK_HOME/conf/spark-env.sh

```shell
YARN_CONF_DIR=/opt/module/hadoop-3.1.3/etc/hadoop

export SPARK_HISTORY_OPTS="
-Dspark.history.ui.port=18080 
-Dspark.history.fs.logDirectory=hdfs://hadoop102:9820/sparklogs
-Dspark.history.retainedApplications=30"
```

编辑 SPARK_HOME/conf/spark-defaults.conf

```
spark.eventLog.enabled true
#需要手动创建
spark.eventLog.dir hdfs://hadoop102:9820/sparklogs
#spark历史服务的地址和端口
spark.yarn.historyServer.address=hadoop103:18080
spark.history.ui.port=18080
```



### 4.2 测试

spark-shell --master yarn:  交互式环境

```scala

```

spark-submit:  用来提交jar包

用法： spark-submit  [options]   jar包   jar包中类的参数

```shell

```

```

```



# 4.Flink运行架构

## 1.RDD的介绍

RDD：  RDD在Driver中封装的是计算逻辑，而不是数据！

​			  使用RDD编程后，调用了行动算子后，此时会提交一个Job，在提交Job时会划分Stage，划分之后，将每个阶段的每个分区使用一个Task进行计算处理！

​			Task封装的就是计算逻辑！ 

​			Driver将Task发送给Executor执行，Driver只是将计算逻辑发送给了Executor!Executor在执行计算逻辑时，此时发现，我们需要读取某个数据，举例(textFile)

​		

​		RDD真正被创建是在Executor端！

​		移动计算而不是移动数据，如果读取的数据是HDFS上的数据时，此时HDFS上的数据以block的形式存储在DataNode所在的机器！

​		如果当前Task要计算的那片数据，恰好就是对应的那块数据，那块数据在102，当前Job在102,104启动了Executor，当前Task发送给102更合适！省略数据的网络传输，直接从本地读取块信息！



## 2.RDD的核心特征

```
 一组分区
 - A list of partitions
 
 private var partitions_ : Array[Partition] = _
 
 #计算分区，生成一个数组，总的分区数
 protected def getPartitions: Array[Partition]
 
 每个Task通过compute读取分区的数据
*  - A function for computing each split
 def compute(split: Partition, context: TaskContext): Iterator[T]
 
 
 记录依赖于其他RDD的依赖关系，用于容错时，重建数据
*  - A list of dependencies on other RDDs

针对RDD[(k,v)]，有分区器！
*  - Optionally, a Partitioner for key-value RDDs (e.g. to say that the RDD is hash-partitioned)


针对一些数据源，可以设置数据读取的偏好位置，用来将task发送给指定的节点，可选的
*  - Optionally, a list of preferred locations to compute each split on (e.g. block locations for
*    an HDFS file)
```



## 3.创建RDD

RDD怎么来： ①new

​								直接new

​								调用SparkContext提供的方法

​						 ②由一个RDD转换而来



### 3.1 makeRDD

从集合中构建RDD

```scala
// 返回ParallelCollectionRDD
def makeRDD[T: ClassTag](
      seq: Seq[T],   
      numSlices: Int = defaultParallelism): RDD[T] = withScope {
    parallelize(seq, numSlices)
  }
```



```
val rdd1: RDD[Int] = sparkContext.makeRDD(list)
    //等价
val rdd2: RDD[Int] = sparkContext.parallelize(list)
```



#### 3.1.1 分区数

numSlices: 控制集合创建几个分区！



在本地提交时，defaultParallelism（默认并行度）由以下参数决定：

```scala
override def defaultParallelism(): Int =
// 默认的 SparkConf中没有设置spark.default.parallelism
  scheduler.conf.getInt("spark.default.parallelism", totalCores)
```

默认defaultParallelism=totalCores(当前本地集群可以用的总核数)，目的为了最大限度并行运行！

​											standalone / YARN模式， totalCores是Job申请的总的核数！



本地集群总的核数取决于  ： Local[N]

​				local:  1核

​				local[2]: 2核

​				local[*] : 所有核



#### 3.1.2 分区策略

```scala
 val slices = ParallelCollectionRDD.slice(data, numSlices).toArray

/*
		seq: List(1, 2, 3, 4, 5)
		numSlices: 4
*/
def slice[T: ClassTag](seq: Seq[T], numSlices: Int): Seq[Seq[T]] = {
    if (numSlices < 1) {
      throw new IllegalArgumentException("Positive number of partitions required")
    }
   
    
    /*
    	length:5
        numSlices: 4
        
        [0,4)
        { (0,1),(1,2),(2,3),(3,5)   }
    */
    def positions(length: Long, numSlices: Int): Iterator[(Int, Int)] = {
        [0,4)
      (0 until numSlices).iterator.map { i =>
        val start = ((i * length) / numSlices).toInt
        val end = (((i + 1) * length) / numSlices).toInt
        (start, end)
      }
    }
    seq match {
      // Range特殊对待
      case _ =>
        // ｛1，2，3，4，5｝
        val array = seq.toArray // To prevent O(n^2) operations for List etc
        
        //{ (0,1),(1,2),(2,3),(3,5)   }
        positions(array.length, numSlices).map { case (start, end) =>
            {1}
            {2}
            {3}
            {4,5}
            array.slice(start, end).toSeq
        }.toSeq
    }
  }
```

总结：  ParallelCollectionRDD在对集合中的元素进行分区时，大致是平均分。如果不能整除，后面的分区会多分！



### 3.2 textfile

textfile从文件系统中读取文件，基于读取的数据，创建HadoopRDD！



#### 3.2.1 分区数

```scala
def textFile(
      path: String,
      minPartitions: Int = defaultMinPartitions): RDD[String] = withScope {
    assertNotStopped()
    hadoopFile(path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text],
      minPartitions).map(pair => pair._2.toString).setName(path)
  }
```



defaultMinPartitions:

```scala
// 使用defaultParallelism(默认集群的核数) 和 2取最小
def defaultMinPartitions: Int = math.min(defaultParallelism, 2)
```



defaultMinPartitions和minPartitions 不是最终 分区数，但是会影响最终分区数！



最终分区数，取决于切片数！

#### 3.2.2 分区策略

```scala
override def getPartitions: Array[Partition] = {
    val jobConf = getJobConf()
    // add the credentials here as this can be called before SparkContext initialized
    SparkHadoopUtil.get.addCredentials(jobConf)
    try {
        // getInputFormat() 获取输入格式
      val allInputSplits = getInputFormat(jobConf).getSplits(jobConf, minPartitions)
      val inputSplits = if (ignoreEmptySplits) {
        allInputSplits.filter(_.getLength > 0)
      } else {
        allInputSplits
      }
      
        // 切片数 就是分区数
      val array = new Array[Partition](inputSplits.size)
      for (i <- 0 until inputSplits.size) {
        array(i) = new HadoopPartition(id, i, inputSplits(i))
      }
      array
    }
  }
```



切片策略：

```java
public InputSplit[] getSplits(JobConf job, int numSplits)
    throws IOException {
    StopWatch sw = new StopWatch().start();
    FileStatus[] files = listStatus(job);
    
    // Save the number of input files for metrics/loadgen
    job.setLong(NUM_INPUT_FILES, files.length);
    
    // 计算所有要切的文件的总大小
    long totalSize = 0;                           // compute total size
    for (FileStatus file: files) {                // check we have valid files
      if (file.isDirectory()) {
        throw new IOException("Not a file: "+ file.getPath());
      }
      totalSize += file.getLen();
    }

    // goalSize： 目标大小  numSplits=minPartitions
    long goalSize = totalSize / (numSplits == 0 ? 1 : numSplits);
    // minSize: 默认1
    long minSize = Math.max(job.getLong(org.apache.hadoop.mapreduce.lib.input.
      FileInputFormat.SPLIT_MINSIZE, 1), minSplitSize);

    // generate splits
    ArrayList<FileSplit> splits = new ArrayList<FileSplit>(numSplits);
    NetworkTopology clusterMap = new NetworkTopology();
    
    // 遍历要切片的文件
    for (FileStatus file: files) {
      Path path = file.getPath();
      long length = file.getLen();
        
        //如果文件不为空，依次切片
      if (length != 0) {
        FileSystem fs = path.getFileSystem(job);
        BlockLocation[] blkLocations;
        if (file instanceof LocatedFileStatus) {
          blkLocations = ((LocatedFileStatus) file).getBlockLocations();
        } else {
          blkLocations = fs.getFileBlockLocations(file, 0, length);
        }
          
          //判断文件是否可切，如果可切，依次切片
        if (isSplitable(fs, path)) {
            
            // 获取块大小  默认128M
          long blockSize = file.getBlockSize();
            
            // 获取片大小
          long splitSize = computeSplitSize(goalSize, minSize, blockSize);

          long bytesRemaining = length;
          while (((double) bytesRemaining)/splitSize > SPLIT_SLOP) {
            String[][] splitHosts = getSplitHostsAndCachedHosts(blkLocations,
                length-bytesRemaining, splitSize, clusterMap);
            splits.add(makeSplit(path, length-bytesRemaining, splitSize,
                splitHosts[0], splitHosts[1]));
            bytesRemaining -= splitSize;
          }

          if (bytesRemaining != 0) {
            String[][] splitHosts = getSplitHostsAndCachedHosts(blkLocations, length
                - bytesRemaining, bytesRemaining, clusterMap);
            splits.add(makeSplit(path, length - bytesRemaining, bytesRemaining,
                splitHosts[0], splitHosts[1]));
          }
        } else {
            // 文件不可切，整个文件作为1片
          String[][] splitHosts = getSplitHostsAndCachedHosts(blkLocations,0,length,clusterMap);
          splits.add(makeSplit(path, 0, length, splitHosts[0], splitHosts[1]));
        }
          // 文件为空，空文件单独切一片
      } else { 
        //Create empty hosts array for zero length files
        splits.add(makeSplit(path, 0, length, new String[0]));
      }
    }
    sw.stop();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Total # of splits generated by getSplits: " + splits.size()
          + ", TimeTaken: " + sw.now(TimeUnit.MILLISECONDS));
    }
    return splits.toArray(new FileSplit[splits.size()]);
  }
```



片大小的计算：

```java
long splitSize = computeSplitSize(goalSize, minSize, blockSize);

// 默认blockSize=128M    goalSize： 46
return Math.max(minSize, Math.min(goalSize, blockSize));
```



总结：  一般情况下，blockSize =  splitSize，默认文件有几块，切几片！

## 4.RDD的算子分类

单Value类型的RDD：  RDD[value]

K-V类型的RDD：  RDD[(k,v)]

双Value类型的RDD：   RDD[value1]      RDD[value2]

### 4.1 单Value类型的RDD使用算子

#### 4.1.1map

```scala
def map[U: ClassTag](f: T => U): RDD[U] = withScope {
    // 当f函数存在闭包时，将闭包进行清理，确保使用的闭包变量可以被序列化，才能发给task
    val cleanF = sc.clean(f)
    
    // iter 使用scala集合中的迭代器，调用 map方法，迭代集合中的元素，每个元素都调用 f
    new MapPartitionsRDD[U, T](this, (_, _, iter) => iter.map(cleanF))
  }
```

MapPartitionsRDD





#### 4.1.2 mapPartitions

```scala
def mapPartitions[U: ClassTag](
      f: Iterator[T] => Iterator[U],
      preservesPartitioning: Boolean = false): RDD[U] = withScope {
    val cleanedF = sc.clean(f)
    new MapPartitionsRDD(
      this,
      (_: TaskContext, _: Int, iter: Iterator[T]) => cleanedF(iter),
      preservesPartitioning)
  }
```

MapPartitionsRDD



```scala
//分区整体调用一次函数
mapPartitions:  (_: TaskContext, _: Int, iter: Iterator[T]) => cleanedF(iter)

// 分区中的每一个元素，调用一次f（函数）
map:  (this, (_, _, iter) => iter.map(cleanF)
```

区别：

①mapPartitions(批处理) 和 map（1对1）

②某些场景下，只能使用mapPartitions

​			将一个分区的数据写入到数据库中！

③ map（1对1） ，只能对每个元素都进行map处理

​		mapPartitions(批处理)，可以对一个分区的数据进行任何类型的处理，例如filter等其他算子都行！

​		返回的记录可以和之前输入的记录个数不同！



#### 4.1.3 mapPartitionsWithIndex

mapPartitions 为每个分区的数据提供了一个对应分区的索引号。



#### 4.1.4 flatMap

扁平化



#### 4.1.5 glom

glom将一个分区的数据合并到一个 Array中，返回一个新的RDD

```
def glom(): RDD[Array[T]] = withScope {
  new MapPartitionsRDD[Array[T], T](this, (_, _, iter) => Iterator(iter.toArray))
}
```



#### 4.1.6 shuffle

shuffle：  在Hadoop的MR中，shuffle意思为混洗，目的是为了在MapTask和ReduceTask传递数据！

​					在传递数据时，会对数据进行分区，排序等操作！



​					当Job有reduce阶段时，才会有shuffle!



Spark中的shuffle：

①spark中只有特定的算子会触发shuffle，shuffle会在不同的分区间重新分配数据！

​		如果出现了shuffle，会造成需要跨机器和executor传输数据，这样会造成低效和额外的资源消耗！



②和Hadoop的shuffle不同的时，数据分到哪些区是确定的，但是在区内的顺序不一定有序！

​	如果希望shuffle后的数据有序，可以以下操作：

​		a) 调用mapPartitions,对每个分区的数据，进行手动排序！

​		b)repartitionAndSortWithinPartitions

​		c)sortBy

​		

③什么操作会导致shuffle

​		a）重新分区的算子 ： reparition, collase

​		b)  xxxBykey类型的算子，除了 count(统计)ByKey

​		c）  join类型的算子，例如[`cogroup`](http://spark.apache.org/docs/latest/rdd-programming-guide.html#CogroupLink) and [`join`](http://spark.apache.org/docs/latest/rdd-programming-guide.html#JoinLink).



④在Spark中，shuffle会带来性能消耗，主要涉及  磁盘IO,网络IO，对象的序列化和反序列化！

​	在Spark中，基于MR中提出的MapTask和ReduceTask概念，spark也将shuffle中组织数据的task称为

​	maptask,将聚合数据的task称为reduceTask!



​	maptask和spark的map算子无关，reducetask和reduce算子无关！



​	Spark的shuffle，mapTask将所有的数据先缓存如内存，如果内存不足，将数据基于分区排序后，溢写到磁盘！ ReduceTask读取相关分区的数据块，组织数据！

​	mapTask端在组织数据时，如果内存不够，导致磁盘溢写，触发GC！



​	Spark的shuffle同时会在磁盘上产生大量的溢写的临时文件，这个临时文件会一直保留，直到后续的RDD完全不需要使用！此举是为了避免在数据容错时，重新计算RDD，重新产生shuffle文件！



​	长时间运行的Spark的Job，如果在shuffle中产生大量的临时的磁盘文件，会占用大量的本地磁盘空间，可以通过spark.local.dir设置本地数据保存的目录！



总结： ①Spark的shuffle和Hadoop的shuffle目的都是为了 在不同的task交换数据！

​			 ②Spark的shuffle借鉴了hadoop的shuffle，但是在细节上略有不同

​					hadoop的shuffle：  数据被拉取到ReduceTask端时，会经过排序！

​					在Spark中，shuffle在ReduceTask端，拉取数据后不会排序！

​			③shuffle会消耗性能，因此能避免就避免，避免不了，采取一些优化的策略！

​	

#### 4.1.7 groupby

```
def groupBy[K](f: T => K)(implicit kt: ClassTag[K]): RDD[(K, Iterable[T])] = withScope {
  groupBy[K](f, defaultPartitioner(this))
}
```

将RDD中的元素通过一个函数进行转换，将转换后的类型作为KEY，进行分组！

#### 4.1.8 defaultPartitioner(this)

defaultPartitioner： 为类似cogroup-like类型的算子，选择一个分区器！在一组RDD中，选择一个分区器！



如何确定新的RDD的分区数：  

​			如果设置了并行度，使用并行度作为分区数，否则使用上游最大分区数

​			

如何确定分区器：

​			在上述确定了分区数后，就默认使用上游最大分区数所在RDD的分区器！如果分区器可以用，就使用！

​			

​			否则，就使用HashPartitioner，HashPartitioner使用上述确定的分区数，进行分区！





总结：  默认不设置并行度，取上游最大分区数，作为下游分区数。

​			 默认取上游最大分区数的分区器，如果没有，就使用HashPartitioner!



```scala
def defaultPartitioner(rdd: RDD[_], others: RDD[_]*): Partitioner = {
    //  将所有的RDD，放入到一个Seq中
    val rdds = (Seq(rdd) ++ others)
    /*
    	_.partitioner:  rdd.partitioner   =  Option[Partitioner]
    	
    	_.partitioner.exists(_.numPartitions > 0) 返回true: 
    			_.partitioner 不能为 none ,代表 RDD有分区器
    			且
    			分区器的总的分区数 > 0 
    			
   		hasPartitioner: Seq[Rdd] :  都有Partitioner，且分区器的总的分区数 > 0 
    			
    */  
    val hasPartitioner = rdds.filter(_.partitioner.exists(_.numPartitions > 0))

    // 取上游分区数最大的RDD  hasMaxPartitioner：要么为none，要么是拥有最大分区数和分区器的RDD
    val hasMaxPartitioner: Option[RDD[_]] = if (hasPartitioner.nonEmpty) {
      Some(hasPartitioner.maxBy(_.partitions.length))
    } else {
      None
    }

    // 如果设置了默认并行度，defaultNumPartitions=默认并行度，否则等于上游最大的分区数！
    val defaultNumPartitions = if (rdd.context.conf.contains("spark.default.parallelism")) {
      rdd.context.defaultParallelism
    } else {
      rdds.map(_.partitions.length).max
    }

    // If the existing max partitioner is an eligible one, or its partitions number is larger
    // than or equal to the default number of partitions, use the existing partitioner.
    /*
    	如果上游存在有最大分区数的RDD有分区器，且(这个分区器可用 或 默认分区数 <= 上游存在有最大分区数的RDD的分区数)  就使用 上游存在有最大分区数的RDD的分区器，否则，就new HashPartitioner(defaultNumPartitions)
    */
    if (hasMaxPartitioner.nonEmpty && (isEligiblePartitioner(hasMaxPartitioner.get, rdds) ||
        defaultNumPartitions <= hasMaxPartitioner.get.getNumPartitions)) {
      hasMaxPartitioner.get.partitioner.get
    } else {
      new HashPartitioner(defaultNumPartitions)
    }
  }
```



#### 4.1.9  Partitioner

```scala
abstract class Partitioner extends Serializable {
    // 数据所分的总区数
  def numPartitions: Int
    // 返回每个元素的分区号
  def getPartition(key: Any): Int
}
```



spark默认提供两个分区器：

HashPartitioner:  调用元素的hashCode方法，基于hash值进行分区！

```scala

```

RangeParitioner:   局限：针对数据必须是可以排序的类型！

​				将数据，进行采样，采样（水塘抽样）后确定一个范围(边界)，将RDD中的每个元素划分到边界中！

​				采样产生的边界，会尽可能保证RDD的元素在划分到边界后，尽可能均匀！





RangeParitioner 对比 HashPartitioner的优势：  一定程序上，可以避免数据倾斜！

#### 4.1.10 sample

```scala
/*
   withReplacement： 是否允许一个元素被重复抽样
   		true： PoissonSampler算法抽样
   		false： BernoulliSampler算法抽样
   		
   fraction： 抽取样本的大小。
   			withReplacement=true： fraction >=0 
   			withReplacement=false :  fraction : [0,1]
   			
   seed: 随机种子，种子相同，抽样的结果相同！
*/
def sample(
    withReplacement: Boolean,
    fraction: Double,
    seed: Long = Utils.random.nextLong): RDD[T]
```



#### 4.1.11 distinct

```scala

```

去重！有可能会产生shuffle，也有可能没有shuffle！

有shuffle，原理基于reduceByKey进行去重！



可以使用groupBy去重，之后支取分组后的key部分！

#### 4.1.12 依赖关系

narrow dependency： 窄依赖！

wide(shuffle) dependency： 宽（shuffle）依赖！ 宽依赖会造成shuffle！会造成阶段的划分！



#### 4.1.13 coalesce

```scala
def coalesce(numPartitions: Int, shuffle: Boolean = false,
             partitionCoalescer: Option[PartitionCoalescer] = Option.empty)
            (implicit ord: Ordering[T] = null)
    : RDD[T]
```

coalesce: 可以将一个RDD重新划分到若干个分区中！是一个重新分区的算子！

​			默认coalesce只会产生窄依赖！默认只支持将多的分区合并到少的分区！

​			如果将少的分区，扩充到多的分区，此时，coalesce产生的新的分区器依旧为old的分区数，不会变化！



​			如果需要将少的分区，合并到多的分区，可以传入 shuffle=true，此时会产生shuffle！



#### 4.1.14 repartition

repartition： 重新分区的算子！一定有shuffle！

​			如果是将多的分区，核减到少的分区，建议使用collase，否则使用repartition



#### 4.1.15 ClassTag

泛型为了在编译时，检查方法传入的参数类型是否复合指定的泛型（类型检查）



泛型在编译后，泛型会被擦除。 如果在Java代码中，泛型会统一使用Object代替，如果scala会使用Any代替！

在运行时，不知道泛型的类型！



针对Array类型，实例化一个数组时，必须知道当前的数组是个什么类型的数组！

java:   String []

scala：  Array[String]



Array和泛型共同组成数组的类型！ 在使用一些反射框架实例化一个数组对象时，必须指定数组中的泛型！

需要ClassTag,来标记Array类型在擦除前，其中的泛型类型！



#### 4.1.16 filter

```scala
def filter(f: T => Boolean): RDD[T] = withScope {
    val cleanF = sc.clean(f)
    new MapPartitionsRDD[T, T](
      this,
      (_, _, iter) => iter.filter(cleanF),
      preservesPartitioning = true)
  }
```

将RDD中的元素通过传入的函数进行计算，返回true的元素可以保留！



#### 4.1.17 sortBy

```scala
def sortBy[K](
    f: (T) => K,
    ascending: Boolean = true,
    numPartitions: Int = this.partitions.length)
    (implicit ord: Ordering[K], ctag: ClassTag[K]): RDD[T] = withScope {
  this.keyBy[K](f)
      .sortByKey(ascending, numPartitions)
      .values
}
```

使用函数将RDD中的元素进行转换，之后基于转换的类型进行比较排序，排序后，再返回对应的转换之前的元素！



本质利用了sortByKey,有shuffle！

sortByKey在排序后，将结果进行重新分区时，使用RangePartitioner!



对于自定义类型进行排序：

​		①将自定义类型实现Ordered接口

​		②提供对自定义类型排序的Ordering



#### 4.1.18 pipe

pipe允许一个shell脚本来处理RDD中的元素！



在shell脚本中，可以使用READ函数读取RDD中的每一个数据，使用echo将处理后的结果输出！

每个分区都会调用一次脚本！

### 4.2 双Value类型



#### 4.2.3 substract

两个RDD取差集，产生shuffle！取差集合时，会使用当前RDD的分区器和分区数！



#### 4.2.4 cartesian

两个RDD做笛卡尔积，不会产生shuffle！ 运算后的RDD的分区数=所有上游RDD分区数的乘积。



#### 4.2.5 zip

将两个RDD，相同分区，相同位置的元素进行拉链操作，返回一个(x,y)

要求： 两个RDD的分区数和分区中元素的个数必须相同！



#### 4.2.6 zipWithIndex

当前RDD的每个元素和对应的index进行拉链，返回(ele,index)



#### 4.2.7 zipPartitions

两个RDD进行拉链，在拉链时，可以返回任意类型的元素的迭代器！更加灵活！



### 4.3 key-value类型

key-value类型的算子，需要经过隐式转换将RDD[(k,v)]转换为 PairRDDFunctions,才可以调用以下算子

#### 4.3.1 reduceByKey

```
def reduceByKey(func: (V, V) => V): RDD[(K, V)] = self.withScope {
  reduceByKey(defaultPartitioner(self), func)
}
```

会在Map端进行局部合并（类似Combiner）

注意：合并后的类型必须和之前value的类型一致！



#### 4.3.2 aggregateByKey

```scala

def aggregateByKey[U: ClassTag]
(zeroValue: U)
(seqOp: (U, V) => U,
    combOp: (U, U) => U): RDD[(K, U)] = self.withScope {
  aggregateByKey(zeroValue, defaultPartitioner(self))(seqOp, combOp)
}
```



#### 4.3.3 foldByKey

```scala
def foldByKey(zeroValue: V)(func: (V, V) => V): RDD[(K, V)] = self.withScope {
  foldByKey(zeroValue, defaultPartitioner(self))(func)
}
```

foldByKey是aggregateByKey的简化版！

​		foldByKey在运算时，分区内和分区间合并的函数都是一样的！



如果aggregateByKey，分区内和分区间运行的函数一致，且zeroValue和value的类型一致，可以简化使用foldByKey！



#### 4.3.4  combineByKey

```scala

def combineByKey[C](
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiners: (C, C) => C): RDD[(K, C)] = self.withScope {
  combineByKeyWithClassTag(createCombiner, mergeValue, mergeCombiners)(null)
}
```



combineByKeyWithClassTag的简化版本，简化在没有提供ClassTag

combineByKey: 是为了向后兼容。兼容foldByKey类型的算子！



#### 4.3.5 4个算子的区别

```scala
/*
	用函数计算zeroValue
	分区内计算
	分区间计算
	
*/
rdd.combineByKey(v => v + 10, (zero: Int, v) => zero + v, (zero1: Int, zero2: Int) => zero1 + zero2)

//如果不需要使用函数计算zeroValue，而是直接传入，此时就可以简化为

rdd.aggregateByKey(10)(_ + _, _ + _)

//如果aggregateByKey的分区内和分区间运算函数一致，且zeroValue和value同一类型，此时可以简化为

rdd.foldByKey(0)(_ + _)

// 如果zeroValue为0，可以简化为reduceByKey

rdd.reduceByKey(_ + _)


//四个算子本质都调用了，只不过传入的参数进行了不同程度的处理
// 以上四个算子，都可以在map端聚合！
combineByKeyWithClassTag(createCombiner, mergeValue, mergeCombiners)(null)

// 根据需要用，常用是reduceByKey
```

#### 4.3.6 partitionBy

使用指定的分区器对RDD进行重新分区！



自定义Partitioner，自定义类，继承Partitioner类！实现其中的getPartition()



#### 4.3.7 mapValues

将K-V类型RDD相同key的所有values经过函数运算后，再和Key组成新的k-v类型的RDD



#### 4.3.8 groupByKey

根据key进行分组！



#### 4.3.9 SortByKey

根据key进行排序！ 

​		针对自定义的类型进行排序！可以提供一个隐式的自定义类型的排序器Ordering[T]



#### 4.3.10 连接

Join:  根据两个RDD key，对value进行交集运算！





LeftOuterJoin:  类似 left join，取左侧RDD的全部和右侧key有关联的部分！

RightOuterJoin: 类似 right join，取右侧RDD的全部和左侧key有关联的部分！

FullOuterJoin： 取左右RDD的全部，及有关联的部分！

​		如果关联上，使用Some，如果关联不上使用None标识！

#### 4.3.11 cogroup

将左右两侧RDD中所有相同key的value进行聚合，返回每个key及对应values的集合！

将两侧的RDD根据key进行聚合，返回左右两侧RDD相同的values集合的RDD！



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



# 7. Flink流处理高阶编程

# 8.Flink流处理高阶编程实战

# 9.Flink CEP编程

# 10.Flink CEP编程实战

