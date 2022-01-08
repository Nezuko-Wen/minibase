# minibase
Hbase迷你版

### 写入
跳表作为顺序存储结构  
LSM树进行存储:内存中一部分数据，磁盘中一部分数据  
写入数据时先写入写缓存，当写缓存达到一定量时刷盘，因为内存很小磁盘很大，所以在读取文件时可能会无法读取整个文件，所以在存储时需要分块存储。  
存储文件diskfile设计以下几个模块  
* datastore kv数据块
* indexstore 索引信息
* metastore 文件元数据