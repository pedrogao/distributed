# kvraft

## 难点

![线性一致性](https://user-images.githubusercontent.com/32640567/119603839-900e0180-be20-11eb-9f74-8b39705bc35e.png)

## 提示

调用 Start() 后，您的 kvserver 将需要等待 Raft 完成协议。 已同意的命令到达 applyCh。  
当 PutAppend() 和 Get() 处理程序使用 Start() 将命令提交到 Raft 日志时，您的代码将需要继续阅读 applyCh。  
注意 kvserver 和它的 Raft 库之间的死锁。

您可以向 Raft ApplyMsg 添加字段，也可以向 Raft RPC（例如 AppendEntries）添加字段，但是对于大多数实现而言，这不是必需的。
如果 kvserver 不是多数的一部分，则不应完成 Get() RPC（这样它就不会提供陈旧的数据）。  
 一个简单的解决方案是在 Raft 日志中输入每个 Get()（以及每个 Put() 和 Append()）。  
 您不必实现第 8 节中描述的只读操作的优化。

最好从一开始就添加锁定，因为避免死锁的需要有时会影响整体代码设计。 使用 go test -race 检查您的代码是否无竞争。

## 参考资料

- https://pdos.csail.mit.edu/6.824/labs/lab-kvraft.html
