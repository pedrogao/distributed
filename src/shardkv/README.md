# shard kv

## 提示

将代码添加到 server.go 以定期从 shardctrler 获取最新配置，并添加代码以在接收组不负责客户端密钥的分片时拒绝客户端请求。你仍然应该通过第一个测试。

您的服务器应该使用服务器不负责的密钥（即其分片未分配给服务器组的密钥）向客户端 RPC 响应 ErrWrongGroup 错误。确保您的 Get、Put 和 Append 处理程序在面对并发重新配置时正确做出此决定。

按顺序一次重新配置一个进程。
如果测试失败，请检查 gob 错误（例如“gob: type not registered for interface ...”）。 Go 并不认为 gob 错误是致命的，尽管它们对实验室来说是致命的。

您需要为跨分片移动的客户端请求提供最多一次语义（重复检测）。
想想 shardkv 客户端和服务器应该如何处理 ErrWrongGroup。如果客户端收到 ErrWrongGroup，是否应该更改序列号？如果服务器在执行 Get/Put 请求时返回 ErrWrongGroup，是否应该更新客户端状态？

在服务器迁移到新配置后，它可以继续存储它不再拥有的分片（尽管这在真实系统中会令人遗憾）。这可能有助于简化您的服务器实现。

当组 G1 在配置更改期间需要来自 G2 的分片时，在其处理日志条目期间 G2 将分片发送到 G1 的哪个时间点是否重要？

您可以在 RPC 请求或回复中发送整个地图，这可能有助于保持分片传输代码的简单性。

如果您的一个 RPC 处理程序在其回复中包含作为服务器状态一部分的映射（例如键/值映射），您可能会因竞争而遇到错误。 RPC 系统必须读取地图才能将其发送给调用者，但它没有持有覆盖地图的锁。但是，您的服务器可能会在 RPC 系统正在读取同一个映射时继续修改它。解决方案是让 RPC 处理程序在回复中包含地图的副本。

如果您将映射或切片放入 Raft 日志条目，并且您的键/值服务器随后在 applyCh 上看到该条目并将对映射/切片的引用保存在您的键/值服务器的状态中，那么您可能会遇到竞争。制作地图/切片的副本，并将副本存储在您的键/值服务器的状态中。竞赛是在修改 map/slice 的 key/value 服务器和 Raft 在持久化日志的同时读取它。

在配置更改期间，一对组可能需要在它们之间双向移动分片。如果您看到死锁，这是一个可能的来源。

## TODO

- TestMissChange

## 参考资料

- https://www.jianshu.com/p/f5c8ab9cd577
- https://www.cnblogs.com/pxlsdz/p/15685837.html
- https://zhuanlan.zhihu.com/p/464097239
- https://pdos.csail.mit.edu/6.824/labs/lab-shard.html
- https://zhuanlan.zhihu.com/p/463146544
- https://sworduo.github.io/2019/08/16/MIT6-824-lab4-shardKV/
- https://www.cnblogs.com/pxlsdz/p/15685837.html
- https://zhuanlan.zhihu.com/p/464097239
- https://pdos.csail.mit.edu/6.824/labs/lab-shard.html
