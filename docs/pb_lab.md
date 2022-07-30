# primary backup

The viewservice monitors whether each available server is alive or dead.
If the current primary or backup becomes dead, the viewservice selects a server to replace it.
A client checks with the viewservice to find the current primary.
The servers cooperate with the viewservice to ensure that at most one primary is active at a time.

viewservice 监视每个可用的服务器是活着还是死了。
如果当前的主或备份失效，viewservice 会选择一个服务器来替换它。
客户端检查 viewservice 以查找当前主节点。
服务器与 viewservice 合作以确保一次最多有一个主服务器处于活动状态。





