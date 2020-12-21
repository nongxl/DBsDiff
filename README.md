# DBsDiff
#####比对两个数据库之间的库、表以及列的差异。适用于开发库和正式库的比对。
#####分别在config1和config2中配置开发库A和正式库B的连接地址然后运行。

### 实现功能：
1. 首先比对库的差异，为两个库做差集得到的结果；
2. 对共有的库做表的比对，为同名库中表的差集；
3. 对共有的表做列的比对，为共有表的列做差集。

### 注意：
1. 出于安全原因，程序不做结构同步，比对出的差异需要手动alter调整；
2. 程序结构写得不好，可能会产生大量查询和运行效率低的问题;
3. 只对比库、表、字段的多与少，如字段类型等属性差异暂时没有进行比较.


### 后续实现：
1.  将来有需要还会继续实现字段其他信息的比对；
2.  优化输出以及显示方式；
3.  优化查询和处理方式。


