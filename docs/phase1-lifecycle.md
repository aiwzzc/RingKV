# 第一轮重构：配置与生命周期

本轮保留现有网络模型、命令执行路径和配置文件语法。重点是让启动、失败回收、停止和资源所有权可以从代码中直接读出来。网络层与引擎的全面拆分在第二轮进行。

## 建议阅读顺序

1. `src/main.cc`：唯一职责是调用应用入口。
2. `src/Application.cc`：配置 → 信号管理 → 日志 → 服务 → 等待退出。
3. `server/config.h/.cc`：有默认值的配置、严格解析、合并与校验。
4. `server/WorkerGroup.h/.cc`：所有 worker 的准备、错误汇总、停止与销毁协调。
5. `server/kvserver.h/.cc`：一个 worker 内部对象如何组装，以及成员声明顺序的含义。

## 所有权

```text
Application::run 的作用域
  TerminationSignals
  LoggingLifetime
  RingKVServer
    const Config
    topology（分片地址表；拥有者是 RingKVServer）
    WorkerGroup（最后声明，最先析构）
      工作线程上的 unique_ptr<Worker>
        kvserver
          const Config
          JemallocWrapper
          RouteBatchTaskPool
          rdict（包括该分片的过期索引）
          KvsProtocolHandler
          TcpServer（包含 EventLoop、连接与缓冲区池）
```

C++ 成员按声明顺序构造、逆序析构。分配器先声明，保证它比依赖它的池和引擎活得更久。工作线程创建和销毁自己的 `kvserver`，主线程只协调生命周期。

`topology` 中的指针是借用关系，不拥有对象。启动门打开后，所有分片地址都已准备完成；停止时，所有分片先共同完成任务排空，然后才允许任何一个分片销毁。

本轮取消了 `KvsProtocolHandler::config_`、全局过期表和线程本地任务池。只读命令注册表仍保留静态初始化。日志系统目前仍使用原有管理器，但启动和停止由应用作用域显式控制；本轮没有重写整个日志库。

## 启动

```text
读取和校验配置（此时没有 worker、监听 socket、日志后台线程）
  → 阻塞 SIGINT/SIGTERM，随后创建的线程继承信号屏蔽
  → 启动日志
  → 创建 WorkerGroup
  → 每个线程：创建 kvserver → prepare()
  → 主线程等待每个已创建线程报告准备结果
  → 全部成功：打开运行门，开始接收和处理请求
```

`prepare()` 会预留监听端口并建立 I/O 资源，但不会调用业务事件循环。构造 `Acceptor` 本身不再绑定端口。配置中的 IPv4 地址现在会真正传给 bind，默认配置值为 `127.0.0.1`。

任意 worker 构造或准备失败，错误会带上 worker 编号和阶段回到主线程；其他线程走相同的清理流程，程序返回非零状态。线程创建只成功一部分的情况，也按实际创建数量协调。

`Worker` 的少量虚函数用于表达生命周期，也允许用不依赖 io_uring 的替身验证初始化失败。它不是通用依赖注入容器。只有 `requestStop()` 可由控制线程调用，其余生命周期方法均在所属线程执行。

## 停止

```text
主线程通过 sigtimedwait 收到停止信号，或某个 worker 运行失败
  → requestStop：设置原子停止标志并唤醒事件循环
  → 各线程退出事件循环
  → 提交剩余 SQE，同步取消该 ring 的在途操作
  → 回收完成事件，关闭连接并打破 connection/session 引用环
  → 等待所有 worker 停止外部 I/O
  → 各线程排空自己的任务队列
  → 如果这一轮有人执行过任务，再进行一轮全局排空
  → 没有任何任务执行后，允许销毁 worker
  → join 所有工作线程
  → 排空并停止日志线程
```

不能简单执行“退出循环 → join → 释放”：线程 A 的路由任务可能还要把回复或池对象归还到线程 B。全局排空期间，所有 worker、引擎、连接使用的池都保持存活。

第一轮的停止语义是**取消连接 I/O 后安全回收资源**。它不会等待每个客户端收到最后一条回复，也不是承诺持久化数据的优雅停机。已经进入跨线程队列的任务会继续排空，但断开的连接不会再发送回复。带期限的回复排空、取消传播及持久化刷盘属于后续设计。

普通客户端断开时，连接也会等待读写完成，再释放操作使用的缓冲区。池对象归还时重置，销毁时调用真正的 C++ 析构函数。引擎销毁、DEL 和 FLUSHALL 也会释放其拥有的 key/value。

## 配置

保留 `key value` 格式，支持空白行、缩进注释及行尾 `#` 注释。

优先级：**结构体默认值 < 显式指定的配置文件 < 命令行**。没有 `--config` 时只使用默认值，不隐式读取依赖当前目录的文件。

```bash
./build/bin/RingKV --config config/server.conf --check-config
./build/bin/RingKV --config config/server.conf --threads 2 --dump-config
./build/bin/RingKV --ip 127.0.0.1 --port 6379 --threads 4
```

未知字段、重复字段、错误布尔值、非法数值和无法打开的文件都会报错。最终配置会统一校验，并以不可变值传入服务。`--dump-config` 输出可以直接作为新的配置文件；`--check-config` 仅检查配置，不检查端口或内核能力。

当前 AOF/RDB 初始化与恢复代码未完成，因此 `aof_enabled yes`、`rdb_enabled yes`、`cluster_enabled yes` 或 `is_master no` 会明确拒绝启动。旧配置中的关闭值仍然有效。恢复这些能力时，应先补齐对应生命周期，再解除校验限制。

当前使用的 io_uring 标志及同步取消要求 Linux 6.1+、liburing 2.3+，并要求运行环境允许 io_uring。启动时会检查实际能力；内核版本号并不保证容器策略允许使用。同步取消发生不可恢复的内核错误时，程序终止，不能冒险释放仍被内核访问的内存。

## 验证与本轮边界

保留三组小型回归检查：配置 CLI；worker 构造/准备/运行失败和跨线程排空；真实服务端口冲突、读写、连接断开及信号停止。没有新增大型测试框架。

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j
ctest --test-dir build --output-on-failure
```

实际执行过 Debug 构建及 ASan/UBSan 构建的上述检查。ThreadSanitizer 在本次环境报 `unexpected memory mapping`，未获得有效的 TSan 结果。

尚未处理：网络层内部的 AOF/RDB/复制职责、完整配置分组、RESP/路由拆分、批处理写合并的语义问题、命令全集兼容性、背压和带期限的优雅停机。编译器对原有未完成 hash 函数的返回值警告也仍存在。本轮通过的是生命周期检查，不代表整个 KV 已完成正确性验证。
