# Raft 

## PartA：领导者选举

实现 Raft 领导者选举、心跳（leader独有，AppendEntries RPC 但不需要同步日志项）。
目标是实现单个 leader 选举，当没有故障发生时，leader 不变，如果 leader 故障或者
网络包丢失，则会选出新的 leader。

记得使用如下命令进行代码测试：
```shell script
go test -run 2A -race

......
Test (2A): initial election ...
  ... Passed --   3.1  3  116   14386    0
Test (2A): election after network failure ...
  ... Passed --   4.4  3  232   18654    0
Test (2A): multiple elections ...
  ... Passed --   5.7  7 1236  103166    0
PASS
ok      6.824/raft      14.221s
```

注意：一定要加上 -race 来测试并发数据竞争冲突！

