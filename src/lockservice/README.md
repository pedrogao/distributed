# lock service

## 概述

在本实验中，您将构建一个锁定服务，即使服务器出现单个“故障停止”故障，该服务仍可继续运行。

客户端将通过 RPC 与您的锁定服务通信。客户端可以使用两个 RPC 请求：Lock(lockname) 和 Unlock(lockname)。

锁服务维护关于一组开放式命名锁的状态。如果 Lock(lockname) 请求到达服务并且指定的锁没有被持有，或者之前从未使用过，

锁服务应该记住锁被持有，并向客户端返回一个成功的回复。如果锁被持有，服务应该向客户端返回一个不成功的回复。

如果客户端发送 Unlock(lockname)，服务应该将指定的锁标记为未持有。

如果锁被持有，服务应该向客户端返回一个成功的回复；否则回复不成功。

您的锁定服务将在两台服务器上复制，一台主服务器和一台备份服务器。

如果没有失败，客户端将向主节点发送锁定和解锁请求，主节点会将它们转发给备份。

关键是备份保持与主节点相同的状态（即，主节点和备份状态应该就每个锁是持有还是释放达成一致）。

您的系统需要容忍的唯一故障是单个故障停止服务器故障。故障停止失败意味着服务器停止。

本实验中没有其他类型的故障（例如，客户端不故障，网络不故障，所有网络消息都传递到未崩溃的服务器，

服务器仅因停止而不是因计算错误而发生故障）。

在本实验室中，服务器永远不会被修复——一旦主服务器或备份服务器发生故障，它将一直处于故障状态。

如果客户端无法从主服务器获得响应，它应该联系备份服务器。

如果主服务器在转发客户端请求时无法从备份服务器获得响应，则主服务器应停止将客户端请求转发到备份服务器。

本实验中的故障模型（无网络故障，无服务器修复）非常有限，以至于您的锁定服务在实践中不会有太大用处；

随后的实验室将能够容忍更广泛的故障。

## 提示

首先修改 client.go 以便 Unlock() 是 Lock() 的副本，但修改为发送 Unlock RPC。您可以通过将 log.Printf() 插入 server.go 来测试您的代码。

接下来，修改 server.go 以便 Unlock() 函数解锁锁。这应该只需要几行代码，它会让你通过第一个测试。

修改 client.go，使其首先向主节点发送一个 RPC，如果主节点没有响应，则向备份节点发送一个 RPC。

修改 server.go 以便**主服务器告诉备份有关锁定和解锁操作的信息**。使用 RPC 进行此通信。

现在对客户端发送到主服务器，**主服务器转发到备份服务器**然后崩溃，客户端重新发送 RPC 到备份服务器的情况做点什么——备份服务器现在已经看到了 RPC。

可以假设每个客户端应用程序一次只会调用一次 Clerk.Lock() 或 Clerk.Unlock()。当然，可能有多个客户端应用程序，每个应用程序都有自己的 Clerk。

请记住，Go RPC 服务器框架会为每个收到的 RPC 请求启动一个新线程。因此，如果多个 RPC 同时到达（来自多个客户端），则服务器中可能有多个线程同时运行。

您可能会发现您想要生成具有高概率唯一性的数字。尝试这个：

```go
import "crypto/rand"
import "math/big"
func nrand() int64 {
  max := big.NewInt(int64(1) << 62)
  bigx, _ := rand.Int(rand.Reader, max)
  x := bigx.Int64()
  return x
}
```

追踪错误的最简单方法是插入 log.Printf() 语句，使用 go test > out 将输出收集到一个文件中，然后考虑输出是否符合您对代码应该如何表现的理解。最后一步是最重要的。

如果测试失败，您可能需要查看 test_test.go 中的测试代码以找出失败的场景。

## 难点

case TestPrimaryFail7，如下：

```go
func TestPrimaryFail7(t *testing.T) {
	fmt.Printf("Test: Primary failure just before reply #6 ...\n")
	runtime.GOMAXPROCS(4)

	phost := port("p")
	bhost := port("b")
	p := StartServer(phost, bhost, true)  // primary
	b := StartServer(phost, bhost, false) // backup

	ck1 := MakeClerk(phost, bhost)
	ck2 := MakeClerk(phost, bhost)

	tl(t, ck1, "a", true)  // ck1 lock a
	tu(t, ck1, "a", true)  // ck2 unlock a
	tu(t, ck2, "a", false) // ck2 unlock a
	tl(t, ck1, "b", true)  // ck1 lock b

	p.dying = true // p 这次请求仍会处理，但不会回复，且2s后端开，然后dead

	ch := make(chan bool)
	go func() {
		ok := false
		defer func() { ch <- ok }()
		// 此处，发送了 unlock 请求，且 p、b 更新了元数据，但 p 无法 ack，因此客户端无法知道 unlock 成功
		// 2s后，p 请求失败，此时 ck2 立马请求 b，但此时的 b 已经被下面的 lock 了，然后立马又 unlock
		tu(t, ck2, "b", true) // 2 second delay until retry
		ok = true
	}()
	time.Sleep(1 * time.Second)
	tl(t, ck1, "b", true) // ck1 lock b，只会更新 b

	ok := <-ch
	if ok == false {
		t.Fatalf("re-sent Unlock did not return true")
	}
	// 然后此时的 unlock 失败了，因此超时的 b 被 unlock 了
	tu(t, ck1, "b", true) // ck1 unlock b

	b.kill()
	fmt.Printf("  ... Passed\n")
}
```

方案，对加锁的值以`版本`的方式来存储，版本按照`时间戳`来排序，如下：

```go
type version struct {
	Seqno  int64 // 序号
	Val    bool  // 值
	Backup bool  // 是否备份
}
```

其实在分布式中，时间戳是不可靠的，客户端之间的时间并不一致，因此一般会使用`逻辑时钟`，但在这个 lab 中，时间戳就足够了，注意用的是`纳秒`。

另外，版本之间通过时间戳来比较，如果是旧版本，那么按照`旧版本之前最近版本的值`来决定这次 Lock、Unlock 是否成功。

## 结果

```sh
$ go test .
Test: Basic lock/unlock ...
  ... Passed
Test: Primary failure ...
  ... Passed
Test: Primary failure just before reply #1 ...
  ... Passed
Test: Primary failure just before reply #2 ...
  ... Passed
Test: Primary failure just before reply #3 ...
  ... Passed
Test: Primary failure just before reply #4 ...
  ... Passed
Test: Primary failure just before reply #5 ...
  ... Passed
Test: Primary failure just before reply #6 ...
  ... Passed
Test: Primary failure just before reply #7 ...
  ... Passed
Test: Backup failure ...
2022-07-31T23:15:32+08:00 ERROR server.go:168 /var/tmp/824-0/3277318-p update backup fail, times: 5
2022-07-31T23:15:32+08:00 ERROR server.go:91 /var/tmp/824-0/3277318-p update backup fail, times: 5
2022-07-31T23:15:32+08:00 ERROR server.go:168 /var/tmp/824-0/3277318-p update backup fail, times: 5
2022-07-31T23:15:32+08:00 ERROR server.go:168 /var/tmp/824-0/3277318-p update backup fail, times: 5
  ... Passed
Test: Multiple clients with primary failure ...
  ... Passed
Test: Multiple clients, single lock, primary failure ...
lock=0 nl=8772 nu=8716 locked=false
--- FAIL: TestConcurrentCounts (5.00s)
    test_test.go:481: lock race 1
FAIL
FAIL    pedrogao/distributed/lockservice        24.038s
FAIL
```

`TestConcurrentCounts` 这个 case 出现数据 race 的情况，是因为这个 case 本身就有并发问题，`Lock`和`Unlock`加上一把大锁就不可能出现并发问题。

## 参考资料

- [How to generate a random string of a fixed length in Go?](https://stackoverflow.com/questions/22892120/how-to-generate-a-random-string-of-a-fixed-length-in-go/22892986#22892986)
