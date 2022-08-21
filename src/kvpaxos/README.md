# kv-paxos

## 概述

现在您将构建 kvpaxos，一个容错键/值存储系统。您将修改 kvpaxos/client.go、kvpaxos/common.go 和 kvpaxos/server.go。

您的 kvpaxos 副本应该保持不变；唯一的例外是，如果某些副本无法访问，它们可能会落后于其他副本。
如果一个副本在一段时间内无法访问，但随后开始可以访问，它最终应该会赶上（了解它错过的操作）。

您的 kvpaxos 客户端代码应该尝试它知道的不同副本，直到有响应为止。作为大多数可以相互访问的副本的一部分的 kvpaxos 副本应该能够为客户端请求提供服务。

您的存储系统必须为使用其客户端接口的应用程序提供顺序一致性。  
也就是说，对 kvpaxos/client.go 中的 Clerk.Get()、Clerk.Put() 和 Clerk.Append() 方法的完整应用程序调用必须看起来以相同的顺序影响所有副本，  
并且最多 -一次语义。 Clerk.Get() 应该看到最近的 Clerk.Put() 或 Clerk.Append() （按此顺序）写入同一键的值。  
这样做的一个后果是您必须确保对 Clerk.Put() 或 Clerk.Append() 的每个应用程序调用必须按该顺序仅出现一次（即，仅写入一次键/值数据库），  
即使在您的客户端内部也是如此.go 可能必须多次发送 RPC，直到找到一个回复的 kvpaxos 服务器副本。

这是一个合理的计划：

在 server.go 中的 Op 结构中填写 kvpaxos 将使用 Paxos 为每个客户端请求达成一致的“值”信息。  
操作字段名称必须以大写字母开头。您应该使用 Op 结构体作为商定的值。

例如，您应该将 Op 结构体传递给 Paxos Start()。  
Go 的 RPC 可以编组/解组 Op 结构； StartServer() 中对 gob.Register() 的调用教它如何操作。

在 server.go 中实现 PutAppend() 处理程序。  
它应该在 Paxos 日志中输入一个 Put 或 Append Op（即，使用 Paxos 分配一个 Paxos 实例，  
其值包括键和值（以便其他 kvpaxose 知道 Put() 或 Append()））。  
Append Paxos 日志条目应该包含 Append 的参数，但不包含结果值，因为结果可能很大。

实现一个 Get() 处理程序。它应该在 Paxos 日志中输入 Get Op，  
然后“解释”该点之前的日志，以确保其键/值数据库反映所有最近的 Put()。

添加代码以处理重复的客户端请求，包括客户端向一个 kvpaxos 副本发送请求、等待回复超时以及将请求重新发送到另一个副本的情况。  
客户端请求应该只执行一次。请确保您的重复检测方案可以快速释放服务器内存，例如让客户端告诉服务器它已经收到了哪些 RPC 的回复。  
可以在下一个客户端请求中捎带此信息。

提示：你的服务器应该尝试将下一个可用的 Paxos 实例（序列号）分配给每个传入的客户端 RPC。但是，  
其他一些 kvpaxos 副本也可能试图将该实例用于不同客户端的操作。所以 kvpaxos 服务器必须准备好尝试不同的实例。

提示：您的 kvpaxos 服务器不应直接通信；它们应该只通过 Paxos 日志相互交互。

提示：与实验 2 一样，您需要唯一标识客户端操作以确保它们只执行一次。同样与实验 2 一样，您可以假设每个职员只有一个未完成的 Put、Get 或 Append。

提示：如果 kvpaxos 服务器不是多数的一部分，则不应完成 Get() RPC（这样它就不会提供陈旧的数据）。  
这意味着每个 Get()（以及每个 Put() 和 Append()）都必须涉及 Paxos 协议。

提示：当 kvpaxos 处理了一个实例并且不再需要它或任何以前的实例时，不要忘记调用 Paxos Done() 方法。

提示：您的代码将需要等待 Paxos 实例完成协议。这样做的唯一方法是定期调用 Status()，在调用之间休眠。  
睡多久？一个好的计划是先快速检查，然后再慢慢检查：

```go
to := 10 * time.Millisecond
  for {
    status, _ := kv.px.Status(seq)
    if status == paxos.Decided{
      ...
      return
    }
    time.Sleep(to)
    if to < 10 * time.Second {
      to *= 2
    }
  }
```

提示：如果您的其中一台 kvpaxos 服务器落后（即在某些情况下没有参与协议），它稍后需要找出同意的内容（如果有的话）。  
一个合理的方法是调用 Start()，这将发现先前商定的值，或者导致协议发生。考虑一下在这种情况下传递给 Start() 的值是多少。

提示：当测试失败时，检查日志中的 gob 错误（例如“rpc: writing response: gob: type not registered for interface ...”），  
因为 go 不认为错误是致命的，尽管它对于实验室。

## 结果

```sh
go test -race .      
ok      pedrogao/distributed/kvpaxos    79.032s
```

## 参考资料

-[lab3](http://nil.csail.mit.edu/6.824/2015/labs/lab-3.html)
