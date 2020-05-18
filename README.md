# MiniSQL
This is the repository for our database final project - MiniSQL



2020/5/18

第一次更新。

目前状况：除了IndexManager以外，其它模块已经搭好大框架。简单的表可以实现index之外的所有正常操作，只要不过分地蹂躏语法，应该还是不会太报错的。

剩余问题：

- Interpreter：select，insert，delete还没有处理换行符，应该不是很困难
  - 一些语法检查比较粗糙而且代码不精炼，可以改进，至少保证语法检查更全面一些
  - 没有做返回的行数的打印，没有处理成员函数的返回值。应该也不会很困难。
  - 没有做花费时间的部分
- BufferManager：LRU的部分选择逻辑似乎有一些问题，而且还没有测过大规模用到超过一个块的数据，只是简单可以跑了。

