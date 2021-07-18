TaskManager是进程级别的，那么一个Flink任务会有一个TM还是多个TM？

对应的

​	yarn-session模式共用JM和TM，那么提交多个任务都用一个TM的话，slot会够吗？

​	如果slot是TM的一个线程，如果只有一个TM，那么所有的slot肯定在一个节点上，那么分布式计算不就实现不了了？



调优方面

​	并行度压测，是否可以说并没有进行实际压测，只是懂原理？



maxwell每次任务开启是同步增量数据，在有些flink任务没有跑的时候也会产生业务数据在mysql，那么应该是每次启动业务相关flink任务时都要使用bootstrap 更新mysql旧数据？