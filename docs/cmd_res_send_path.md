## 关于网络层的 Command Response 的路径设计

### Design Goal
发送的载体是struct ReplyBuffer

ReplyBuffer用于解决网络层 Command Response 的高频小包发送问题

核心目标：

1. 避免频繁堆内存分配
2. 减少字符串拷贝
3. 支持 io_uring writev 聚合发送
4. 保证发送期间数据生命周期安全
5. 避免多线程/多协程下写入竞争
6. 支持高吞吐 backlog 排队
7. 尽可能利用 inline small buffer 优化小包

### Buffer的设计
- 第一个版本：
```cpp
struct ReplyBuffer {

    GeneralHead head_;  // 公用head用于区分不同Io事件
    struct list_head node_; // 侵入式链表池节点

    TcpConnectionPtr conn_;
    bool in_used_;

    char inline_buf_[META_BUF_SIZE]; // 栈空间 
    std::size_t inline_offset_ = 0;

    struct iovec iovs_[MAX_IOV_COUNT]; // io_uring_writev 优化
    int iov_cnt_;
    std::size_t total_bytes_;
    std::size_t iov_start_idx_;

    std::vector<std::string> string_holders_; // 保证数据声明周期

    // 将数据保存于Buffer中
    void appendString(std::string&& data) {
        std::size_t len = data.size();

        if(len < 128 && inline_offset_ + len <= META_BUF_SIZE)
        ::memcpy(inline_buf_ + inline_offset_, data, len);
        
        // 优化点: 
        // 如果栈空间的可写首地址 - 当前struct iovec.iov_len == 当前struct iovec.iov_base
        // 只需要将当前struct iovec.iov_len += len;即可
        // 但是优化效果有限，因为只有一个栈空间，如果栈满了还是需要push_back到string_holders_中

        // 如果inline_buf_满了
        // string_holders_.emplace_back(std::move(data));
        // 虽然是move但初始化时还是拷贝了
    }
};
```
### Partial Write Handling
io_uring_writev 完成后可能仅发送部分数据

因此 ReplyBuffer 需要维护：

- 当前发送 iov index
- 当前 iov offset
- 剩余 total_bytes

onComplete(res) 后：

1. 消费已发送字节
2. 推进 iovec cursor
3. 若未发送完成则继续提交 writev
4. 全部发送完成后归还 BufferPool

### 具体流程
因为 io_uring_writev 后内核态还会持有Buffer的指针，因此用户态不能修改不能析构否则io_uring异步DMA阶段可能访问非法内存，TcpConnection同一时间维护了两个struct ReplyBuffer：
- current_reply_ 和 writing_reply_；current_reply_用于不断将数据保存在Buffer中，writing_reply_用于实际的io_uring_writev操作，此时TcpConnection->is_writing_ = true;保证同一时间只有一个Buffer处于发送阶段，避免逻辑死锁
- 如果处于is_writing_ = true状态，当current_reply_处理完后会暂存在TcpConnection->std::deque<ReplyBufferPtr> backlog_;队列中；当事件循环处理此次事件且writing_reply_数据完全发送成功后会从backlog_中取出Buffer充当writing_reply_再次调用io_uring_writev；

流程图
```cpp
        data                                                reply满了 || cmd_res处理完了
engine ------> TcpConnection->current_reply_(从Pool中获取) ------------------------------->
                            writing_reply_ =  backlog_.empty() ? current_reply_ : backlog_.front()
TcpConnection->send_start() ---------------------------------------------------------------------->
                                                            事件就绪后
io_uring_writev() && io_uring_sqe_set_data(writing_reply_) -----------> EventLoop 不断调用 writing_reply_->onComplete()

```

状态机
```cpp
                +------------------+
                |    Idle          |
                +------------------+
                         |
                         | append response
                         v
                +------------------+
                | current_reply_   |
                +------------------+
                         |
                         | send_start()
                         v
                +------------------+
                | writing_reply_   |
                | is_writing=true  |
                +------------------+
                         |
             +-----------+-----------+
             |                       |
             | partial write         | write complete
             v                       v
    continue io_uring_writev   backlog empty?
                                       |
                            +----------+---------+
                            |                    |
                            | yes                | no
                            v                    v
                         Idle           pop backlog
```

### trade off
- TcpConnection->std::deque<ReplyBufferPtr> backlog_存在的意义：网络发送速度 < 业务生成速度；所以backlog_本质是发送队列是应用层的 send buffer
- 使用struct iovec是为了避免数据拼接拷贝
- 当前用户态缓存采用std::vector<>如果发生扩容会导致iovec.iov_base地址失效,后续这个版本将改为std::deque或者优化为chain buffer