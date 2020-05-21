# 查询模块设计文档
## SQL解析

使用`Antlr4`的`Visitor`模式解析SQL语句。实现继承自`SQLBaseVisitor`类的子类`MySQLVisitor`。

定义抽象类`Statement`，用来存储语句解析后的相关信息。通过`MySQLVisior`类的解析，返回一个`Statement`类的列表。不同类型的SQL语句对应不同的`Statement`的子类，如`Select`语句返回`SelectStatement`类的对象。

在解析的过程中，还定义了许多对应SQL语句中关键词的类，辅助完成解析，并封装保存需要返回的结果。

## SQL执行
### SQL执行器
在`Manager`中实现并封装了一个`SQLExecutor`类来执行SQL语句，可以执行建表、删表、插入、删除、更新等文档中要求的操作，为了方便测试还支持了创建数据库、选择数据库、删除数据库等操作。在`SQLExecutor`中传入`Statement`（各种操作分别对应不同的`Statement`子类），利用`Statement`中的操作表、列名、数据、表达式等信息完成对数据库的操作，操作的同时检查是否满足数据库的一致性、完整性要求，对相应的错误进行处理。

### Query操作
Query操作中用`MetaInfo`记录操作所用到的数据表的元数据，`QueryTable`用作迭代器从数据表中读取数据给操作逻辑。

Query的过程在`QueryResult.querySelect\Update\Delete`方法中，该方法首先通过`QueryTable`获取要操作的数据表的`RowList`（可以是两个表条件Join后的`RowList`），然后对其中的每一列计算条件表达式`Condition`是否为真，将结果为真的列加入结果列表。

### 表达式求值
计算条件表达式`Condition`首先计算条件运算符两边`Expression`的值，再根据`Expression`的返回值结合条件运算符计算`Condition`的值。

#### Expression求值
`Expression`可能含有两个操作数和一个操作符或者一个操作数，对于涉及到表内容的操作数需要利用`MetaInfo`获取该操作数在`Row`中的index，然后从`Row`中获取值；对于常数操作数则直接运算。`Expression`计算的结果以`Double`（对操作数进行四则运算的结果）或`String`（直接获取表中STRING列的值）类型返回。

#### Condition求值
求解完条件运算符两边`Expression`的值后，首先判断两边的值类型是否相同，类型不同则报错；如果类型相同执行条件运算并返回结果。

