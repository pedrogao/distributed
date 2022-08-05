# paxos

> paxos 分布式共识性算法学习与实践

## 笔记

- 准备阶段 Prepare 不需要携带提议值，只需携带提议编号即可；
- 由于之前没有通过任何提案，所以节点 A、B 将返回一个 “尚无提案”的响应。  
  也就是说节点 A 和 B 在告诉提议者，我之前没有通过任何提案呢，并承诺以后不再响应提案编号小于等于 1 的准备请求，不会通过编号小于 1 的提案。
- 当节点 A、B 收到提案编号为 5 的准备请求的时候，因为提案编号 5 大于它们之前响应的准备请求的提案编号 1，  
  而且两个节点都没有通过任何提案，所以它将返回一个 “尚无提案”的响应，并承诺以后不再响应提案编号小于等于 5 的准备请求，不会通过编号小于 5 的提案。

## 参考资料

1. Paxos vs raft: Have we reached consensus on distributed consensus?
   https://arxiv.org/pdf/2004.05074.pdf#:~:text=Most%20notably%2C%20Raft%20only%20allows,is%20up%2Dto%2Ddate.
2. Implementing Replicated Logs with Paxos
   https://ongardie.net/static/raft/userstudy/paxos.pdf
   youtube: https://www.youtube.com/watch?v=JEpsBg0AO6o
3. Paxos Made Moderately Complex
   https://paxos.systems/
4. http://nil.csail.mit.edu/6.824/2015/labs/lab-3.html
