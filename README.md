# RingKV
一款基于 Shared-nothing 架构与 io_uring 网络设施的高性能多线程KV存储引擎

当前正在进行架构重构。第一轮已整理配置和启动/退出流程，阅读入口见 [第一轮设计说明](docs/phase1-lifecycle.md)。当前可运行模式为单机内存模式，未完成的 AOF、RDB、复制配置会明确拒绝启动。

## Features
- 基于io_uring的高性能网络
- Shared-nothing 多线程架构
- 全面支持Redis Resp协议
- Multi-Proactor event loop model
- Thread Local 侵入式链表池
- Lock-free MPSC SPSC
- Batch Request Routing
- 自研异步日志系统

## Performance

Benchmark tool: `redis-benchmark`

## Build

### Requirements

- Linux 6.1+，运行环境允许 io_uring
- CMake 3.20+
- GCC 13+
- liburing 2.3+
- jemalloc
- hiredis
- Python 3（仅运行测试需要，可用 `-DBUILD_TESTING=OFF` 关闭测试构建）

### Compile

```bash
git clone https://github.com/aiwzzc/RingKV.git
cd RingKV

cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j
```

---

## Run

```bash
./build/bin/RingKV --config config/server.conf --check-config
./build/bin/RingKV --config config/server.conf
```

支持 `--ip`、`--port`、`--threads` 覆盖配置，以及 `--dump-config` 查看最终值。不指定配置文件时使用默认值（`127.0.0.1:5005`，1 个 worker）。使用 Ctrl+C 或 SIGTERM 停止服务；当前停止会取消连接 I/O，不保证所有在途回复送达。

运行本轮回归检查：`ctest --test-dir build --output-on-failure`。

---

---

## Supported RESP Commands

支持 string list hash set zset 5种数据结构命令交互,具体与Redis一致

---

## Design Highlights

### Shared-Nothing

可以理解为每个线程就是一个KV引擎;command parse后通过对Key进行hash分片到不同线程中;下沉到 engine 层处理后的回复通过 RuninLoop思想重新路由回原线程

- 采用Stable Hash进行分片;保证同一个Key被路由到一个EventLoop中
- thread local 侵入式连接池;对需要复用的对象都做了池化;最大限度减少malloc的次数同时避免跨线程free的开销

这样可以最大限度地减少锁争用,提高可扩展性

### io_uring Networking

借鉴Muduo的回调api风格,底层使用io_uring做EventLoop实现Proactor模型

- Read Buffer采用block buffer也就是fixed size buffer;这在命令解析和回复可以做到0拷贝;同样也做了池化处理,但使用的是SPSC做缓存,当然也可以使用侵入式池化
- Write Buffer使用struct iovec配合fixed size buffer;最大限度减少malloc次数;使用Writev减少系统调用;效果就是支持批量提交回复
- 后续引入c++20协程提高代码可维护性

### Route Batch Optimization

为了提高pipeline模式下的吞吐和性能,每个ringClient(对TcpConnection和业务的抽象)分配一个CommandSlots,也就是为每个命令预留一个槽位;这样可以保证命令回复是顺序的;后续其它EventLoop解析完Command后将回归到指定的Slot中

- 后续将对非pipeline模式进行优化
