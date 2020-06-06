主要记录FLink使用的一些心得和使用场景案例
1、Operator State使用



知识点：
一、FlinkOperatorState
1、RichSinkFunction 继承AbstractRichFunction 和实现了SinkFunction
2、继承AbstractRichFunction，提供了初始化和关闭两个方法open(Context)、close(Context),同时可以访问运行时状态的上下文Context
3、实现checkpointFunction，提供initializeState(FunctionInitializationContext context)和snapshotState(FunctionSnapshotContext context)方法
4、生命周期
    a、initializeState创建实例，初始化state数据结构
    b、open初始化方法，可用于初始化变量
    c、invoke，当有数据流进来时触发
    d、snapshotState，当检查点触发时调用，即checkpoint时调用，用于设置checkpoint的state结果
    e、close，实例销毁时调用
    note：当一些需要在restore过程中要用的变量，需要在initializeState初始化


二、DisOrderData
1、生产water有两种形态
    a、AssignerWithPunctuatedWatermarks


待续：
1、乱序场景watermark的使用
2、jdbc 2pc
3、广播变量
4、维表的获取