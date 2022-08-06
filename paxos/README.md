# paxos

## 概述

Basic Paxos 算法将所谓节点一视同仁，既可以是提案者又可以是接受者，因此可能存在两个节点交叉提案形成成为**活锁**。

而且，Basic Paxos 就*一个值*的共识而言，效率都比较低下，需要两次协商（prepare、accept）然后才会达成共识。

所以 Basic Paxos 基本不会在真实场景下落地，在理论上虽然是正确的，但是无法 cover 住真实场景的打磨。

因此 Multi Paxos 提出了一些优化 Basic Paxos 算法的方案：

- 选主，通过 Basic Paxos 选出 leader，然后由 leader 提案，这样就不会存在多个节点提案的情况，减少冲突；
  leader 提案也无需两轮协商，二者由 leader 直接发送共识值，直接进入 accept 阶段，跳过了 prepare 阶段。
- 随机化超时时间，解决两个节点交叉提案造成火锁。

## 参考资料

- [Paxos 算法详解](https://zhuanlan.zhihu.com/p/31780743)
- [分布式共识算法](http://icyfenix.cn/distribution/consensus/)
