# groupcacheX
golang cache project build base on groupcache &amp; TTLCache

缓存系统设计目标：

缓存根据map + 链表做

-------------

自动过期：（可以通过用户token访问接口获取在线用户（assume it's offline after user stop operation about 5 minutes））
          usage: we may send message to users for business according to online status

自动过期基础上续期：token续期 避免操作过程中认证失败（好的体验应该是打开app时告知用户认证失败登录签证）

多点缓存：通过通信共享内存

采用LRU内存管理：Least Recently Used
        查看数据状态

数据一致性：

防缓存击穿：如果某一时间一个 key 失效了，但同时又有大量的请求访问这个 key，此时这些请求都会直接落到下游的DB

防缓存穿透：当请求访问的数据是一条并不存在的数据时，一般这种不存在的数据是不会写入 cache，所以访问这种数据的请求都会直接落地到下游 db
        1：可以考虑适当的缓存这种数据一小段时间，将这种空数据缓存为一段特殊的值。
        2：BloomFilter
        3：全量加载数据库

防缓存雪崩：当因为某种原因，比如同时过期、重启等，大量缓存在同一时间失效而导致大量的请求直接打到下游的服务或DB
        1：做好一致性，不设置过期及取到的数据就是最新数据
        2：数据item设置随机过期
        3：二级缓存
        4：限流

*列表缓存：

*搜索缓存：



