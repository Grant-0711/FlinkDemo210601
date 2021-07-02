# 时间语义

Flink1.12 之前默认是处理时间，Flink1.12之后默认是事件时间。

# 水印

测量事件时间进度的方式（流中特殊的数据，用来表示时间）（和窗口一起使用）



取出数据中的时间戳，形成一个水印，形成水印的时间越早越好。

一般在map之后生成水印，因为map之后可以取出数据中包含时间的信息。（map之前也可以，但是需要自行取出时间戳）

```java
.map(----------------------------------------)
.assignTimestampsAndWatermarks(//StreamExecutionEnvironment调用的方法
    WatermarkStrategy.//是一个接口，有三个静态方法
                   			       <WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))//传入的时间是容忍度
                .withTimestampAssigner((SerializableTimestampAssigner<WaterSensor>)
                                       //然后需要分配时间戳
                        (waterSensor, l) -> waterSensor.getTs()))
```

 

WatermarkStrategy的三个静态方法：

```java
static <T> WatermarkStrategy<T> forBoundedOutOfOrderness(Duration maxOutOfOrderness) {//用于乱序数据
        return (ctx) -> {
            return new BoundedOutOfOrdernessWatermarks(maxOutOfOrderness);
        };
static <T> WatermarkStrategy<T> forMonotonousTimestamps() {
    //用于有序数据
        return (ctx) -> {
            return new AscendingTimestampsWatermarks();
        };
    }
```



## 产生水印的源码

WatermarkStrategy的静态方法：

```
static <T> WatermarkStrategy<T> forBoundedOutOfOrderness(Duration maxOutOfOrderness) {}
```

**WatermarkStrategy<T>是一个函数式接口，函数式接口是只要一个抽象方法的接口**

```java
//返回值是lambda表达式，所以WatermarkStrategy<T>是一个函数式接口，函数式接口是只要一个抽象方法的接口
static <T> WatermarkStrategy<T> forBoundedOutOfOrderness(Duration maxOutOfOrderness) {
        return (ctx) -> {
            return new BoundedOutOfOrdernessWatermarks(maxOutOfOrderness);
        };
    }
```

```java
BoundedOutOfOrdernessWatermarks //是一个构造器
    //它的所属类：
    public class BoundedOutOfOrdernessWatermarks<T> implements WatermarkGenerator<T> {}

```

```java
public class BoundedOutOfOrdernessWatermarks<T> implements WatermarkGenerator<T> {} //这个类的代码：

-------------------------------------------------------------------------
public class BoundedOutOfOrdernessWatermarks<T> implements WatermarkGenerator<T> {
    private long maxTimestamp;
    private final long outOfOrdernessMillis;

    public BoundedOutOfOrdernessWatermarks(Duration maxOutOfOrderness) {
        Preconditions.checkNotNull(maxOutOfOrderness, "maxOutOfOrderness");
        Preconditions.checkArgument(!maxOutOfOrderness.isNegative(), "maxOutOfOrderness cannot be negative");
        this.outOfOrdernessMillis = maxOutOfOrderness.toMillis();
        //把容忍度变成毫秒值存入一个属性中
        
        this.maxTimestamp = -9223372036854775808L + this.outOfOrdernessMillis + 1L;
        //最大的时间戳是Long最小值+容忍度 +1
        //作用是产生了一个很小的时间戳，对之后的数值影响不大。+容忍度 +1的作用是生成水印的时候防止时间戳值
    }

    public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
        this.maxTimestamp = Math.max(this.maxTimestamp, eventTimestamp);
    }//获取当前最大的时间戳，避免时间倒流

    public void onPeriodicEmit(WatermarkOutput output) {
        output.emitWatermark(new Watermark(this.maxTimestamp - this.outOfOrdernessMillis - 1L));
        //周期性的生成水印，默认周期是200ms，水印计算方式是this.maxTimestamp - this.outOfOrdernessMillis - 1L
    }
}
```



## 产生水印的方式

周期性的生成

处理迟到数据的措施：设置容忍度

数据进入哪个窗口和数据本身以及窗口的设置规则有关，不直接决定于水印。水印决定窗口的关闭时间。

多并行度下的水印传递问题

木桶原理，总是传递最小的水印。只是在产生水印时不同，后续传递都相同（第一条数据来产生了水位，可能多个并行度产生的水位是不同的，第二条数据来之后依据木桶原理，选最小的）



## 解决多并行度下数据倾斜的问题

产生原因：

例如Kafka写数据都写入一个分区，另一个分区没数据

问题

导致某个并行度没有数据，窗口不关。

解决办法

1.避免数据倾斜

2.无法避免数据倾斜的话：

​	2.1用shuffle或者rebalance

​	2.2设置超时时间，超时之后自动生成水印

超时时间设置位置：

在.assignTimestampsAndWatermarks(）中进行设置：withIdleness()

```java
                .assignTimestampsAndWatermarks(WatermarkStrategy.
                        <WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner((SerializableTimestampAssigner<WaterSensor>)
                        (waterSensor, l) -> waterSensor.getTs())
----------------------------------------------------------------------   
                .withIdleness())
-----------------------------------------------                
```



源码：

```java
/**
     * Creates a new enriched {@link WatermarkStrategy} that also does idleness detection in the
     可以进行空闲检测的
     * created {@link WatermarkGenerator}.
     *
     * <p>Add an idle timeout to the watermark strategy. If no records flow in a partition of a
     * stream for that amount of time, then that partition is considered "idle" and will not hold
     * back the progress of watermarks in downstream operators.
     *如果在设定的空闲时间内记录的流是空闲的，则认为它是"idle"并且会阻塞到下游流的操作
     * <p>Idleness can be important if some partitions have little data and might not have events
     * during some periods. 
     Idleness在一些数据量小的或者是可能没有事件的流中是很重要的
     Without idleness, these streams can stall the overall event time
     * progress of the application.
     */
//如果超过在idleness传入的时间，则自动往后推进水印

default WatermarkStrategy<T> withIdleness(Duration idleTimeout) {
        checkNotNull(idleTimeout, "idleTimeout");
        checkArgument(
                !(idleTimeout.isZero() || idleTimeout.isNegative()),
                "idleTimeout must be greater than zero");
        return new WatermarkStrategyWithIdleness<>(this, idleTimeout);
    }
```



## 允许迟到数据（窗口才具有的权限）

因此要写在窗口之后

```java
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .allowedLateness(Time.seconds(3))//允许迟到三秒
```

**允许迟到是指达到窗口边界的水印时间时（设置时间要减去1毫秒），窗口不关，但是窗口函数先进行计算，之后再来数据，再进行计算。**

**真正关闭时间是水印时间到窗口边界。**



## 总结解决迟到数据的方法

分两种情况：窗口未关闭和窗口关闭

### 窗口未关闭

产生水印时设置容忍度

```java
WatermarkStrategy.//是一个接口，有三个静态方法
                   			       <WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))//传入的时间是容忍度
```

窗口允许迟到数据

```java
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .allowedLateness(Time.seconds(3))//允许迟到三秒
```

### 窗口关闭情况下采用侧流输出

```java
                .assignTimestampsAndWatermarks(WatermarkStrategy.
                        <WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner((SerializableTimestampAssigner<WaterSensor>)
                        (waterSensor, l) -> waterSensor.getTs())
                .withIdleness(Duration.ofSeconds(10)))
                .keyBy(WaterSensor::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .allowedLateness(Time.seconds(3))
                    --------------------------------------------
                .sideOutputLateData(new OutputTag<WaterSensor>("late"){})
                    //把真正迟到的数据放入侧输出流，加{}的目的是让程序记住泛型
```



## 侧输出流的用法

处理迟到数据（见上）

测输出流的获取方式是调用主流的方法

**getSideOutput**

```java
mainS.getSideOutput(new OutputTag<WaterSensor>("late") {});
```

分流

替换掉之前用的切割流的方法（已经被弃用）

使用方法，在重写的ProcessFunction中调用上下文的output方法，创建OutputTag对象

```java
            .process(new ProcessFunction<WaterSensor, WaterSensor>() {
                @Override
                public void processElement(WaterSensor value,
                                           Context ctx,
                                           Collector<WaterSensor> out) throws Exception {
                    if ("sensor_1".equals(value.getId())) {
                        out.collect(value);
                    } else if ("sensor_2".equals(value.getId())) {
                        -----------------------------------
                        ctx.output(new OutputTag<String>("s2") {}, "s2:" + value.toString());
                        -------------------------------------
                    } else {
                        --------------------------------------
                        ctx.output(new OutputTag<String>("other") {}, "other:" + value.toString());
                        -----------------------------------------

```

# 状态后端

状态后端的作用是维持有状态算子的状态，维持具体指的是访问，存储以及维护

## **状态后端的主要任务：**

本地状态的管理（Taskmanager）

检查点（checkpoint）状态写入远程存储

## **状态后端的分类**

MemoryStateBackend， FsStateBackend 和 RocksDBStateBackend

区别是MemoryStateBackend的checkpoint状态存在JobManager内存中，而FsStateBackend存在文件系统例如hdfs中

RocksDBStateBackend的本地状态存储在TaskManager的RocksDB数据库中(实际是内存+磁盘) ，Checkpoint在外部文件系统(hdfs)中.

## **配置状态后端**

### **全局配置状态后端**

在flink-conf.yaml文件中设置默认的全局后端

### **在代码中配置状态后端**

可以在代码中单独为这个Job设置状态后端.可以在代码中单独为这个Job设置状态后端.

```java
env.setStateBackend(new MemoryStateBackend());
env.setStateBackend(new FsStateBackend("hdfs://hadoop162:8020/flink/checkpoints/fs"));
```

如果要使用RocksDBBackend, 需要先引入依赖:见Flink文档

# Flink中richfunction的一点小作用

泽米 2017-10-12 19:31:46 13901 收藏 13
分类专栏： flink 文章标签： flink
版权

①传递参数
所有需要用户定义的函数都可以转换成richfunction，例如实现map operator中你需要实现一个内部类，并实现它的map方法：

data.map (new MapFunction<String, Integer>() {
  public Integer map(String value) { return Integer.parseInt(value); }
});

    1
    2
    3

然后我们可以将其转换为RichMapFunction：

data.map (new RichMapFunction<String, Integer>() {
  public Integer map(String value) { return Integer.parseInt(value); }
});

    1
    2
    3

当然，RichFuction除了提供原来MapFuction的方法之外，还提供open, close, getRuntimeContext 和setRuntimeContext方法，这些功能可用于参数化函数（传递参数），创建和完成本地状态，访问广播变量以及访问运行时信息以及有关迭代中的信息。
下面我们来看看RichFuction中传递参数的例子，以下代码是测试RichFilterFuction的例子，基于DataSet而非DataStream。
这里写图片描述
由代码可见，可以将Configuration中的limit参数的值传递进RichFuction里面，通过后面withParameters方法传递进去，最后的结果是这里写图片描述
由此可见，我从configuration中获取了limit的值，并设定了fliter的阈值是2，从而过滤了1，2。
②传递广播变量，原理和上面差不多，下面我直接把代码贴出来：
