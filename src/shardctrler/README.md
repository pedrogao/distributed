# shard controller

## 说明

shardctrler 管理一系列版本的配置。每个配置都描述了副本组和分片到副本组的联系。
每当此分配需要更改时，分片控制器都会使用新分配创建一个新配置。

键/值客户端和服务器在想知道当前（或过去）配置时会请求 shardctrler。

您的实现必须支持 shardctrler/common.go 中描述的 RPC 接口，该接口由 Join、Leave、Move 和 Query RPC 组成。  
这些 RPC 旨在允许管理员（和测试）控制 shardctrler：添加新的副本组、消除副本组以及在副本组之间移动分片。

管理员使用 Join RPC 添加新的副本组。它的参数是一组从唯一的非零副本组标识符 (GID) 到服务器名称列表的映射。  
shardctrler 应该通过创建一个包含新副本组的新配置。新配置应在所有组中尽可能均匀地分配分片，
并应移动尽可能少的分片以实现该目标。如果 GID 不是当前配置的一部分，  
则 shardctrler 应该允许重新使用它（即，应该允许 GID 加入，然后离开，然后再次加入）。

Leave RPC 的参数是以前加入的组的 GID 列表。 shardctrler 应该创建一个不包括这些组的新配置，  
并将这些组的分片分配给剩余的组。新配置应在组之间尽可能均匀地划分分片，并应移动尽可能少的分片以实现该目标。

Move RPC 的参数是一个分片号和一个 GID。 shardctrler 应该创建一个新配置，其中将分片分配给组。  
Move 的目的是让我们能够测试您的软件。移动后的加入或离开可能会取消移动，因为加入和离开会重新平衡。

Query RPC 的参数是一个配置号。 shardctrler 回复具有该编号的配置。  
如果该数字为 -1 或大于已知的最大配置数字，则 shardctrler 应回复最新配置。  
Query(-1) 的结果应该反映 shardctrler 在收到 Query(-1) RPC 之前完成处理的每个 Join、Leave 或 Move RPC。

第一个配置应该编号为零。它不应包含任何组，并且所有分片都应分配给 GID 零（无效的 GID）。  
下一个配置（为响应加入 RPC 而创建）应该编号为 1，&c。  
分片通常比组多得多（即每个组将服务多个分片），以便可以以相当精细的粒度转移负载。

## 提示

从您的 kvraft 服务器的精简副本开始。  

您应该为分片控制器的 RPC 实现重复的客户端请求检测。 shardctrler 测试不会对此进行测试，  
但 shardkv 测试稍后会在不可靠的网络上使用您的 shardctrler；   
如果您的 shardctrler 没有过滤掉重复的 RPC，您可能无法通过 shardkv 测试。

状态机中执行分片重新平衡的代码需要是确定性的。 在 Go 中，map 迭代顺序是不确定的。
围棋地图是参考。 如果将一个 map 类型的变量分配给另一个变量，则两个变量都引用同一个 map。   
因此，如果您想基于前一个配置创建一个新的配置，您需要创建一个新的映射对象（使用 make()）并单独复制键和值。

## 参考资料

- https://www.jianshu.com/p/f5c8ab9cd577
- https://www.cnblogs.com/pxlsdz/p/15685837.html
- https://zhuanlan.zhihu.com/p/464097239