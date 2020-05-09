# 元数据模块设计文档
## 文件组织

`Manager`中用`HashMap`存储一组`Databse`，每个`Database`存储在硬盘上时对应一个以`Database`的`name`命名的文件夹。

`Database`中用`HashMap`存储一组`Table`，存储时用一个文件记录所有`Table`的`name`，每个`Table`的元数据占用一个文件。

`Database`初始化时通过`recover()`读取存储在硬盘上的数据库文件，其中包括记录所有`Table`的列表，所有`Table`的元数据，和所有`Table`的记录。退出时通过`persist()`将上述内容存储到硬盘上。

元数据和记录的存储结构如下：

```
DatabaseName(Database文件夹)
│  TableName1_SCHEMA(Table1的元数据)
│  TableName2_SCHEMA(Table2的元数据)
│  TABLES_NAME(Table列表)
│  
└─data(记录)
        TableName1(Table1的记录)
        TableName2(Table2的记录)
```

