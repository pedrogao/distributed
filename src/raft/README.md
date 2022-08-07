# raft

详细参考[raft 实现](../../notes/%E8%B0%88%E8%B0%88%20Raft%20%E5%88%86%E5%B8%83%E5%BC%8F%E5%85%B1%E8%AF%86%E6%80%A7%E7%AE%97%E6%B3%95%E7%9A%84%E5%AE%9E%E7%8E%B0.md)。

测试结果：

```sh
# ./test.sh

Test (2A): initial election ...
  ... Passed --   3.1  3  116   34174    0
Test (2A): election after network failure ...
  ... Passed --   4.5  3  228   47326    0
Test (2A): multiple elections ...
  ... Passed --   5.4  7 1062  219555    0
Test (2B): basic agreement ...
  ... Passed --   0.5  3   16    4628    3
Test (2B): RPC byte count ...
  ... Passed --   1.6  3   54  116538   11
Test (2B): agreement after follower reconnects ...
  ... Passed --   5.4  3  216   60624    8
Test (2B): no agreement if too many followers disconnect ...
  ... Passed --   3.5  5  360   78847    3
Test (2B): concurrent Start()s ...
  ... Passed --   0.7  3   24    7032    6
Test (2B): rejoin of partitioned leader ...
  ... Passed --   4.0  3  252   61608    4
Test (2B): leader backs up quickly over incorrect follower logs ...
  ... Passed --  17.2  5 2632 1983280  103
Test (2B): RPC counts aren't too high ...
  ... Passed --   2.2  3   80   24116   12
Test (2C): basic persistence ...
  ... Passed --   3.2  3  120   33173    6
Test (2C): more persistence ...
  ... Passed --  14.6  5 1616  352724   16
Test (2C): partitioned leader and one follower crash, leader restarts ...
  ... Passed --   1.4  3   44   12273    4
Test (2C): Figure 8 ...
  ... Passed --  30.0  5 1464  311381   38
Test (2C): unreliable agreement ...
  ... Passed --   4.1  5  312  105813  246
Test (2C): Figure 8 (unreliable) ...
  ... Passed --  38.9  5 6520 18933890  121
Test (2C): churn ...
  ... Passed --  16.5  5 1648 1920542  388
Test (2C): unreliable churn ...
  ... Passed --  16.2  5  980  375811  226
Test (2D): snapshots basic ...
  ... Passed --   4.2  3  154   62972  208
Test (2D): install snapshots (disconnect) ...
  ... Passed --  47.0  3 1948  778287  320
Test (2D): install snapshots (disconnect+unreliable) ...
  ... Passed --  54.3  3 2570  919886  319
Test (2D): install snapshots (crash) ...
  ... Passed --  29.5  3 1056  508156  319
Test (2D): install snapshots (unreliable+crash) ...
  ... Passed --  37.5  3 1336  615166  312
Test (2D): crash and restart all servers ...
  ... Passed --   7.9  3  280   90458   59
PASS
ok      pedrogao/distributed/raft       353.477s
```

- leader backs up quickly over incorrect follower logs ...
- snapshots basic ...