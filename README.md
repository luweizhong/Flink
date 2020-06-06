主要记录FLink使用的一些心得和使用场景案例
1、Operator State使用



知识点：
一、RichSinkFunction
1、RichSinkFunction 继承AbstractRichFunction 和实现了SinkFunction
2、继承AbstractRichFunction，提供了初始化和关闭两个方法open(Context)、close(Context),同时可以访问运行时状态的上下文Context
3、





代序：
1、乱序场景watermark的使用
2、jdbc 2pc
3、广播变量
4、维表的获取