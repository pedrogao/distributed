# kvraft

## 难点

![线性一致性](https://user-images.githubusercontent.com/32640567/119603839-900e0180-be20-11eb-9f74-8b39705bc35e.png)

线性一致性解决的是，日志被提交后，但如果出现了网络问题；比如：客户端A发送Set请求后，服务器成功接收到
Set请求，日志也提交成功，但ACK被丢包了；客户端B在下一秒也发出Set请求，客户端A会重试，就会覆盖客户端A
Set请求。

## 提示

### PartA

调用 Start() 后，您的 kvserver 将需要等待 Raft 完成协议。 已同意的命令到达 applyCh。  
当 PutAppend() 和 Get() 处理程序使用 Start() 将命令提交到 Raft 日志时，您的代码将需要继续阅读 applyCh。  
注意 kvserver 和它的 Raft 库之间的死锁。

您可以向 Raft ApplyMsg 添加字段，也可以向 Raft RPC（例如 AppendEntries）添加字段，但是对于大多数实现而言，这不是必需的。
如果 kvserver 不是多数的一部分，则不应完成 Get() RPC（这样它就不会提供陈旧的数据）。  
 一个简单的解决方案是在 Raft 日志中输入每个 Get()（以及每个 Put() 和 Append()）。  
 您不必实现第 8 节中描述的只读操作的优化。

最好从一开始就添加锁定，因为避免死锁的需要有时会影响整体代码设计。 使用 go test -race 检查您的代码是否无竞争。

### PartB

maxraftstate 表示持久 Raft 状态的最大允许大小（以字节为单位）（包括日志，但不包括快照）。
将 maxraftstate 与 persister.RaftStateSize() 进行比较。
每当你的键/值服务器检测到 Raft 状态大小接近这个阈值时，它应该通过调用 Raft 的 Snapshot 来保存一个快照。
如果 maxraftstate 为 -1，则不必进行快照。

考虑一下 kvserver 何时应该对其状态进行快照，以及快照中应该包含哪些内容。
Raft 使用 SaveStateAndSnapshot() 将每个快照存储在持久化对象中，以及相应的 Raft 状态。

可以使用 ReadSnapshot() 读取最新存储的快照。

kvserver 必须能够跨检查点检测日志中的重复操作，因此您用来检测它们的任何状态都必须包含在快照中。

## 参考资料

- https://pdos.csail.mit.edu/6.824/labs/lab-kvraft.html
