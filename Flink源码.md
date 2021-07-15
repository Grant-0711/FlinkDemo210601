# 类：StreamExecutionEnvironment

流程序的运行context

该环境提供了控制作业执行的方法（例如设置并行度或容错/检查点参数）以及与外部世界交互（数据访问）。 

## 包含属性和方法的作用：

​	指定流任务名

​	不指定时间语义的话默认事件时间

​	创建线程存储类存储StreamExecutionEnvironmentFactory

​	创建本地运行环境的时候如果没有指定并行度，默认根据当前jvm核数来决定并行度

```java
private static int defaultLocalParallelism = Runtime.getRuntime().availableProcessors();
```

​	指定运行参数，checkpoint参数

​	bufferTimeOut默认不过期 为 -1L

​	指定checkpoint存储路径

​	设置刷新输出缓冲区的最大时间频率（毫秒）

```java
public StreamExecutionEnvironment setBufferTimeout(long timeoutMillis)
```

设置刷新输出缓冲区的最大时间频率（毫秒）。 默认情况下，输出缓冲区会频繁刷新以提供低延迟并帮助流畅的开发人员体验。 设置参数可以产生三种逻辑模式：

​		正整数触发该整数定期刷新
​		0 在每条记录后触发刷新，从而最大限度地减少延迟
​		-1 仅在输出缓冲区已满时触发刷新，从而最大化吞吐量

# 创建Yarn客户端应用程序

## 入口

CliFrontend.java

```java
public static void main(final String[] args) {
	... ...
	final CliFrontend cli = new CliFrontend
	... ...
int retCode = SecurityUtils.getInstalledContext()
					.runSecured(() -> cli.parseParameters(args));
	... ...
}
public int parseParameters(String[] args) {
	... ...
	// get action
	String action = args[0];

	// remove action from parameters
	final String[] params = Arrays.copyOfRange(args, 1, args.length);
	
		// do action
		switch (action) {
			case ACTION_RUN:
			case ACTION_LIST:case ACTION_INFO:
             case ACTION_CANCEL:case ACTION_STOP:
			case ACTION_SAVEPOINT:case "-h":
			case "--help":
			CliFrontendParser.printHelp(customCommandLines);
			case "-v":
			case "--version":
		}
	... ...
}
```



## 解析参数

CliFrontend.java

```java
protected void run(String[] args) throws Exception {
	... ...
	// 获取默认的运行参数
	final Options commandOptions = CliFrontendParser.getRunCommandOptions();
	// 解析参数，返回commandLine
	final CommandLine commandLine = getCommandLine(commandOptions, args, true);
	... ...
}
public CommandLine getCommandLine(final Options commandOptions, final String[] args, final boolean stopAtNonOptions) throws CliArgsException {
	final Options commandLineOptions = CliFrontendParser.mergeOptions(commandOptions, customCommandLineOptions);
	return CliFrontendParser.parse(commandLineOptions, args, stopAtNonOptions);
}

DefaultParser.java
public class CliFrontendParser {
	// 选项列表
	static final Option HELP_OPTION = new Option("h", "help", false,
			"Show the help message for the CLI Frontend or the action.");

	static final Option JAR_OPTION = new Option("j", "jarfile", true, "Flink program JAR file.");

	static final Option CLASS_OPTION = new Option("c", "class", true,
			"Class with the program entry point (\"main()\" method). Only needed if the " +
			"JAR file does not specify the class in its manifest.");
... ...
}
DefaultParser.java
public CommandLine parse(Options options, String[] arguments, Properties properties, boolean stopAtNonOption)
//解析命令行参数
//解析--后面的参数，解析=后面的参数
各种情况的解析，逻辑大体相同：去除-或--前缀，校验参数，以其中一个为例
private void handleLongOptionWithoutEqual(String token) throws ParseException
{
	// 校验参数是否合法
  
{
// 参数添加到执行命令
        handleOption(options.getOption(matchingOpts.get(0)));
    }
}
Options.java： 
public List<String> getMatchingOptions(String opt)
{
	// 去除 - 或 -- 前缀
    opt = Util.stripLeadingHyphens(opt);
}
DefaultParser.java
private void handleOption(Option option) throws ParseException
{
    // check the previous option before handling the next one
    checkRequiredArgs();

    option = (Option) option.clone();

    updateRequiredOptions(option);

    cmd.addOption(option);

    if (option.hasArg())
    {
        currentOption = option;
    }
    else
    {
        currentOption = null;
```



## 决定客户端类型

CliFrontend.java

```java
public static void main(final String[] args) {
	... ...
	final List<CustomCommandLine> customCommandLines = loadCustomCommandLines(
			configuration,
			configurationDirectory);
	... ...
	final CliFrontend cli = new CliFrontend(
				configuration,
				customCommandLines);
	... ...
}
```

这里依次添加了 Generic、Yarn和Default三种命令行客户端（后面根据isActive()按顺序选择）：

如果没有指定或者指定出问题，就调用default，因此要按照顺序，因为DefaultCLI isActive一直返回true

```java
public static List<CustomCommandLine> loadCustomCommandLines(Configuration configuration, String configurationDirectory) {
	List<CustomCommandLine> customCommandLines = new ArrayList<>();
	customCommandLines.add(new GenericCLI(configuration, configurationDirectory));

	//	Command line interface of the YARN session, with a special initialization here
	//	to prefix all options with y/yarn.
	final String flinkYarnSessionCLI = "org.apache.flink.yarn.cli.FlinkYarnSessionCli";
	try {
		customCommandLines.add(
			loadCustomCommandLine(flinkYarnSessionCLI,
				configuration,
				configurationDirectory,
				"y",
				"yarn"));
	} catch xx) {}
	}

	//	Tips: DefaultCLI must be added at last, because getActiveCustomCommandLine(..) will get the
	//	      active CustomCommandLine in order and DefaultCLI isActive always return true.
	customCommandLines.add(new DefaultCLI(configuration));

	return customCommandLines;
```

在run()里面，进行客户端的选择：

```java
protected void run(String[] args) throws Exception {
	... ...
	final CustomCommandLine activeCommandLine =
				validateAndGetActiveCommandLine(checkNotNull(commandLine));}
```



在FlinkYarnSessionCli为active时优先返回FlinkYarnSessionCli。

对于DefaultCli，它的isActive方法总是返回true。

```java
public CustomCommandLine validateAndGetActiveCommandLine(CommandLine commandLine) {
... ...
	for (CustomCommandLine cli : customCommandLines) {
	... ...
	//在FlinkYarnSessionCli为active时优先返回FlinkYarnSessionCli。
		//对于DefaultCli，它的isActive方法总是返回true。
		if (cli.isActive(commandLine)) {
			return cli;
```



## 获取有效配置

## 调用user main

## 执行sc的execute方法



