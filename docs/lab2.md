# Raft

## PartA：领导者选举

实现 Raft 领导者选举、心跳（leader 独有，AppendEntries RPC 但不需要同步日志项）。
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

## 难点

### 日志

```go
func defaultRLog() rLog {
	return rLog{
		// 第0位当作哨兵处理，即 lastIncluded
		Entries: []LogEntry{{
			Term:    0,
			Command: nil,
		}},
		LastIncludedIndex: 0,
		LastIncludedTerm:  0,
	}
}
```

使用 0 位作为日志哨兵，记录最后一个 lastIncludedTerm 和 lastIncludedIndex，一定要在快照后  
重新设置哨兵日志的任期，不然容易导致日志无法同步。

对日志追加，裁剪，为了避免 data race，采用 copy on write：

```go
func (l *rLog) append(entrys ...LogEntry) {
	// data race: https://zhuanlan.zhihu.com/p/228166716
	// copy 必须是 len，而不是 cap
	tmp := make([]LogEntry, len(l.Entries), len(l.Entries)+len(entrys))
	copy(tmp, l.Entries)
	tmp = append(tmp, entrys...)
	l.Entries = tmp
}
```

通过 copy on write 就能避免切片访问时的数据竞争，另外，`copy` 使用的时候，一定得设置切片的 `len`，避免无法拷贝。

### 快照

leader 发送快照后，更新 match、next index 时，注意比较 lastIncludeIndex：

```go
	// 注意，快照和日志同步一样，需要更新 matchIndex 和 nextIndex
	// 发送完快照后，更新了 matchIndex 和 nextIndex，因此在快照期间的日志同步将需要重新来
	// FIXME? 如果已同步的序号小，才接收快照
	if rf.matchIndex[peerId] < args.LastIncludedIndex {
		rf.matchIndex[peerId] = args.LastIncludedIndex
		rf.nextIndex[peerId] = args.LastIncludedIndex + 1
	}
```

不然就会导致日志做无用功同步。

另外，节点安装快照时：

```go
	rf.commitIndex = maxInt(rf.commitIndex, lastIncludedIndex)
  rf.lastApplied = maxInt(rf.lastApplied, lastIncludedIndex)
```

注意，提交、应用序号的更新。

## 参考资料

- [目前为止见过最好的 Raft 算法实现剖析 6.2 The Raft consensus algorithm](https://www.cl.cam.ac.uk/teaching/2122/ConcDisSys/dist-sys-notes.pdf)
