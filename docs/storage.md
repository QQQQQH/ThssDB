# 存储模块设计文档
## 文件组织
Table中的数据采用Sequential结构存储，每个Table占用一个文件。Table中每一条记录跟B+树的索引绑定，写入文件时采用广度优先搜索遍历B+树，迭代器缓存每一个叶子节点中的记录（实际上是按照索引顺序缓存），然后读取该缓存并用Java序列化写到文件中。
## 缓存组织
在构造Table时在recover()中读取序列化文件，并将记录插入到B+树中。对数据的增删改查都在内存中执行，只有在Table被销毁或者需要释放缓存空间时才将Table中的数据写入文件。