#include "EventLoop.h"
#include "IoRequest.h"
#include "Acceptor.h"
#include "PendingWrite.h"
#include "TcpConnection.h"
#include "src/engine.h"
#include "src/server.h"
#include "src/protocolhandler.h"
#include "src/config.h"

#include <sys/eventfd.h>
#include <cstring>
#include <cstdio>
#include <iostream>
#include <stdio.h>
#include <immintrin.h>

namespace {

    uint64_t getCurrentTimestamp() {
        auto now = std::chrono::system_clock::time_point::clock::now();
        auto duration = now.time_since_epoch();
        auto milliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(duration);
        return milliseconds.count(); //单位是毫秒
    }

};

namespace AeroIO {

namespace net {

static thread_local EventLoop* t_loopInThisThread = nullptr;

EventLoop* EventLoop::getEventLoopOfCurrentThread()
{ return t_loopInThisThread; }

EventLoop::EventLoop(rkv::Ringengine* engine) : 
    engine_(engine), wakeupfd_(createEventfd()), 
    pending_functors_(std::make_unique<moodycamel::ConcurrentQueue<Functor>>()),
    fixed_fds_index_(3), aof_is_flushing_(false), aof_fsync_in_flight_(false), 
    aof_is_rewriting_(false), aof_file_offset_(0), temp_aof_file_offset_(0), 
    aof_fsync_error_count_(0), aof_rewrite_bucket_cursor_(0), rdb_is_saving_(false),
    rdb_cow_bucket_cursor_(0), repl_is_writing_(false) {

    this->fixed_fds_.resize(FIXEDFDSIZE, -1);

    this->threadId_ = std::this_thread::get_id();
    t_loopInThisThread = this;

    io_uring_params params;
    memset(&params, 0, sizeof(io_uring_params));

    // // 开启 SQPOLL，内核线程自动拉取，实现零系统调用
    // params.flags |= IORING_SETUP_SQPOLL;
    // params.sq_thread_idle = 2000; // 空闲2秒后内核线程休眠

    // 魔法 1：告诉内核，这个 Ring 只有一个线程在操作。
    // 内核会直接干掉 io_uring 内部的各种互斥锁和自旋锁！(需 Linux 6.0+)
    params.flags |= IORING_SETUP_SINGLE_ISSUER; 

    // 魔法 2：协同式 Task Work。
    // 告诉内核：不要发中断打断我！我会配合你在 io_uring_enter 的时候自己处理回调。 (需 Linux 5.19+)
    params.flags |= IORING_SETUP_COOP_TASKRUN;  

    // 魔法 3：延迟 Task Work。
    // 强制所有的完成事件，只有当线程主动调用 wait() 时才处理，彻底回归类似 epoll 的无中断轮询模式！ (需 Linux 6.1+)
    params.flags |= IORING_SETUP_DEFER_TASKRUN; 

    params.cq_entries = kEntresLength * 2;
    this->ring_ = new io_uring;
    int ret = io_uring_queue_init_params(kEntresLength, this->ring_, &params);

    if (ret < 0) {
        delete this->ring_;
        this->ring_ = nullptr;
        
        char err_buf[256];
        snprintf(err_buf, sizeof(err_buf), "io_uring_queue_init_params failed: %s (ret=%d)", strerror(-ret), ret);
        
        // LOG_FATAL << err_buf;
        printf("io_uring init ret = %d, err = %s\n", ret, strerror(-ret));
        abort();
    }

    io_uring_register_files(this->ring_, this->fixed_fds_.data(), this->fixed_fds_.size());
    io_uring_register_files_update(this->ring_, 0, &this->wakeupfd_, 1);
    this->fixed_fds_[0] = this->wakeupfd_;
}

EventLoop::~EventLoop() {
    ::close(this->wakeupfd_);
    io_uring_queue_exit(this->ring_);
    delete this->ring_;
}

io_uring* EventLoop::ring()
{ return this->ring_; }

std::vector<int>& EventLoop::getFixedFds()
{ return this->fixed_fds_; }

int EventLoop::getConnIndex()
{ return this->connect_index_; }

rkv::Ringengine* EventLoop::getEngine()
{ return this->engine_; }

void EventLoop::setLoopsEngines(std::vector<std::pair<EventLoop*, rkv::Ringengine*>>* loopsengines)
{ this->LoopsEngines_ = loopsengines; }

std::vector<std::pair<EventLoop*, rkv::Ringengine*>>* EventLoop::getLoopsEngines()
{ return this->LoopsEngines_; }

void EventLoop::queueInfreeFixedFds(int index)
{ this->free_fixed_fds_.push_back(index); }

int EventLoop::getFreeFixedFd() {
    if(this->free_fixed_fds_.empty()) return -1;

    int index = this->free_fixed_fds_.front();
    this->free_fixed_fds_.pop_front();

    return index;
}

const char* EventLoop::getAofFilePath()
{ return this->aof_file_path_.c_str(); }

const char* EventLoop::getRdbFilePath()
{ return this->rdb_file_path_.c_str(); }

std::size_t& EventLoop::getAofFileOffset()
{ return this->aof_file_offset_; }

int EventLoop::getNextIndex() {
    if(this->fixed_fds_index_ + 1 >= MAX_FIXED_FDS) return -1;

    if(this->fixed_fds_index_ + 1 >= this->fixed_fds_.size()) {
        std::size_t new_size = this->fixed_fds_.size() + this->fixed_fds_.size() / 2;
        if(new_size > MAX_FIXED_FDS) new_size = MAX_FIXED_FDS;
        this->fixed_fds_.resize(new_size, -1);
    }

    return this->fixed_fds_index_++;
}

void EventLoop::addTimer(uint64_t ms, const TimerCallback& cb, TimerRequest* timer) {
    io_uring_sqe* sqe = io_uring_get_sqe(this->ring_);
    if(!sqe) {
        io_uring_submit(this->ring_);
        sqe = io_uring_get_sqe(this->ring_);
    }

    timer->setTimer(ms, cb);

    io_uring_prep_timeout(sqe, &timer->ts_, 0, 0);
    io_uring_sqe_set_data(sqe, timer);
}

bool EventLoop::isInLoopThread() const 
{ return std::this_thread::get_id() == this->threadId_; }

void EventLoop::setTcpServer(TcpServer* server)
{ this->TcpServer_ = server; }

TcpServer* EventLoop::getTcpServer()
{ return this->TcpServer_; }

void EventLoop::runInLoop(Functor cb) {
    if(isInLoopThread()) {
        cb();
        return;
    }

    this->pending_functors_->enqueue(std::move(cb));

    std::atomic_thread_fence(std::memory_order_seq_cst);
    
    if(this->is_sleeping_.load(std::memory_order_seq_cst)) {
        wakeup();
    }
}

int EventLoop::createEventfd() {
    int evtfd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (evtfd < 0) {
        abort();
    }
    return evtfd;
}

void EventLoop::wakeup() {
    uint64_t one = 1;
    ssize_t n = ::write(this->wakeupfd_, &one, sizeof one);
    if(n != sizeof one) {
        std::cerr << "Wakeup failed! errno: " << errno << std::endl;
    }
}

void EventLoop::setPersistFileIndex(int index) { 
    this->connect_index_ = index;
    std::string index_str = std::to_string(index);
    this->aof_file_path_ = AOFBASEFILEPATH + index_str + AOFSUFFIXFILEPATH;
    this->temp_aof_path_ = AOFBASEFILEPATH + index_str + AOFTEMPSUFFIXFILEPATH;

    this->rdb_file_path_ = RDBBASEFILEPATH + index_str + RDBSUFFIXFILEPATH;
    this->handShake_instruction_ = HANDSHAKEORDER + index_str + HANDSHAKESUFFIX;
}

void EventLoop::armWakeupFd() {
    io_uring_sqe* sqe = io_uring_get_sqe(this->ring_);
    if(!sqe) {
        io_uring_submit(this->ring_);
        sqe = io_uring_get_sqe(this->ring_);
    }

    io_uring_prep_read(sqe, 0, &this->wakeup_buf_, sizeof(this->wakeup_buf_), 0);
    sqe->flags |= IOSQE_FIXED_FILE;
    io_uring_sqe_set_data(sqe, (void*)WAKEUP_MSG_DATA);
    // std::cout << "post eventfd" << std::endl;
}

void EventLoop::sendMsgRing(EventLoop* senderLoop, int target_ring_fd) {
    io_uring_sqe* sqe = io_uring_get_sqe(senderLoop->ring_);
    if(!sqe) {
        io_uring_submit(senderLoop->ring_);
        sqe = io_uring_get_sqe(senderLoop->ring_);
    }

    // MSG_RING 任务
    /*
    新锐 MSG_RING 唤醒：
    线程 A 只需要在自己的 io_uring SQ（提交队列）中塞入一个特殊的 SQE（类型为 IORING_OP_MSG_RING），
    目标指明是线程 B 的 ring_fd。当线程 A 正常调用 io_uring_submit() 
    批量提交网络包时，内核顺手就会把一个完成事件（CQE）直接“空投”到线程 B 的 CQ 队列里。全程零额外系统调用
    */
    io_uring_prep_msg_ring(sqe, target_ring_fd, 0, WAKEUP_MSG_DATA, 0);
    sqe->msg_ring_flags = IORING_MSG_RING_CQE_SKIP;
    io_uring_sqe_set_data(sqe, (void*)SENDER_IGNORE_MSG);
}

void EventLoop::appendAof(char* data, std::size_t len) {
    this->aof_write_buffer_.append(data, len);

    if(this->aof_is_rewriting_) {
        this->aof_rewrite_buffer_.append(data, len);
    }
}

void EventLoop::tryFlushAof() {
    if(!rkv::Config::getInstance().aof_enabled_) return;

    if(this->aof_write_buffer_.readableBytes() == 0 || 
        this->aof_is_flushing_ || this->aof_fd_ < 0) return;

    if(this->aof_flush_buffer_.readableBytes() == 0 &&
        this->aof_write_buffer_.readableBytes() == 0) return;

    if(this->aof_flush_buffer_.readableBytes() == 0) {
        std::swap(this->aof_flush_buffer_, this->aof_write_buffer_);

    } else {
        this->aof_flush_buffer_.append((char*)this->aof_write_buffer_.peek(), 
            this->aof_write_buffer_.readableBytes());
    }
    
    this->aof_write_buffer_.retrieveAll();
    this->aof_is_flushing_ = true;

    io_uring_sqe* sqe = io_uring_get_sqe(this->ring_);
    if(!sqe) {
        io_uring_submit(this->ring_);
        sqe = io_uring_get_sqe(this->ring_);
    }

    io_uring_prep_write(sqe, 1, this->aof_flush_buffer_.peek(), 
    this->aof_flush_buffer_.readableBytes(), this->aof_file_offset_);
    sqe->flags |= IOSQE_FIXED_FILE;
    io_uring_sqe_set_data(sqe, this->aof_req_.get());
}

void EventLoop::tryFsyncAof() {
    if(this->aof_fsync_in_flight_) {
        std::cerr << "[Warning] Disk is too slow! A previous fsync is still in flight." << std::endl;
        return;
    }

    this->aof_fsync_in_flight_ = true;

    io_uring_sqe* sqe = io_uring_get_sqe(this->ring_);
    if(!sqe) {
        io_uring_submit(this->ring_);
        sqe = io_uring_get_sqe(this->ring_);
    }

    // IORING_FSYNC_DATASYNC 类似于 fdatasync，只刷数据，不刷文件修改时间等元数据，更快
    io_uring_prep_fsync(sqe, this->aof_fd_, IORING_FSYNC_DATASYNC);
    io_uring_sqe_set_data(sqe, this->aoffsync_req_.get());
}

void EventLoop::startAofRewrite() {
    if(this->aof_is_rewriting_) return;

    this->aof_is_rewriting_ = true;
    this->aof_rewrite_bucket_cursor_ = 0;

    this->temp_aof_fd_ = ::open(this->temp_aof_path_.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if(this->temp_aof_fd_ < 0) {
        std::cerr << "Failed to open AOF file: " << this->temp_aof_path_ 
                  << ", Error: " << strerror(errno) << std::endl;

        return;
    }

    auto& map = this->engine_->getUnderlyingMap();
    map.max_load_factor(1000000.0);

    doAofRewriteStep();
}


void EventLoop::doAofRewriteStep() {
#if 0
    if(!this->aof_is_rewriting_) return;

    auto& map = this->engine_->getUnderlyingMap();
    std::size_t total_buckets = map.bucket_count();

    std::size_t step_limit = 100;
    std::size_t processed = 0;

    while(this->aof_rewrite_bucket_cursor_ < total_buckets && processed < step_limit) {
        for(auto it = map.begin(this->aof_rewrite_bucket_cursor_);
            it != map.end(this->aof_rewrite_bucket_cursor_); ++it) {
            
            std::string cmd = rkv::KvsProtocolHandler::dumpObjectToResp(it->first, it->second);;
            this->aof_rewrite_flush_buffer_.append(cmd.data(), cmd.size());
        }

        ++this->aof_rewrite_bucket_cursor_;
        ++processed;
    }

    if(this->aof_rewrite_flush_buffer_.readableBytes() > 0) {
        if(this->aof_rewrite_bucket_cursor_ >= total_buckets && this->aof_is_rewriting_) {
            this->aof_rewrite_flush_buffer_.append((char*)this->aof_rewrite_buffer_.peek(),
            this->aof_rewrite_buffer_.readableBytes());

            this->aof_rewrite_buffer_.retrieveAll();
            this->aof_is_rewriting_ = false;
        }

        tryFlushTempAof();
    }
#endif
}


void EventLoop::tryFlushTempAof() {
    if(!this->aof_is_rewriting_) return;

    io_uring_sqe* sqe = io_uring_get_sqe(this->ring_);
    if(!sqe) {
        io_uring_submit(this->ring_);
        sqe = io_uring_get_sqe(this->ring_);
    }

    io_uring_prep_write(sqe, this->temp_aof_fd_, this->aof_rewrite_flush_buffer_.peek(),
    this->aof_rewrite_flush_buffer_.readableBytes(), this->temp_aof_file_offset_);
    io_uring_sqe_set_data(sqe, this->aofrewrite_req_.get());
}

void EventLoop::finishAofRewrite() {
    if(this->aof_is_rewriting_) return;

    auto& map = this->engine_->getUnderlyingMap();
    map.max_load_factor(1.0);
    map.rehash(0); // 强制执行一次rehash

    ::rename(this->temp_aof_path_.c_str(), this->aof_file_path_.c_str());
    ::close(this->temp_aof_fd_);

    this->aof_fd_ = ::open(this->aof_file_path_.c_str(), O_WRONLY | O_CREAT, 0644);
    if (this->aof_fd_ < 0) {
        std::cerr << "Failed to open AOF file: " << this->aof_file_path_ 
                  << ", Error: " << strerror(errno) << std::endl;
    }

    io_uring_register_files_update(this->ring_, 1, &this->aof_fd_, 1);
    this->fixed_fds_[1] = this->aof_fd_;

    this->aof_file_offset_ = this->temp_aof_file_offset_;
    this->temp_aof_file_offset_ = 0;
    this->aof_rewrite_flush_buffer_.retrieveAll();
    this->aof_rewrite_buffer_.retrieveAll();
}

bool EventLoop::rdbIsSaving()
{ return this->rdb_is_saving_; }

UringBuffer* EventLoop::rdbWriteBuffer()
{ return &this->rdb_write_buffer_; }

std::size_t EventLoop::rdbCowBuchetIndex()
{ return this->rdb_cow_bucket_cursor_; }

std::unordered_set<std::string>& EventLoop::rdbCowSavedKeys()
{ return this->rdb_cow_saved_keys_; }

void EventLoop::startRdb() {
    if(this->rdb_is_saving_) return;

    this->rdb_fd_ = ::open(this->rdb_file_path_.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (this->rdb_fd_ < 0) {
        std::cerr << "Failed to open RDB file: " << this->rdb_file_path_ 
                  << ", Error: " << strerror(errno) << std::endl;

        return;
    }

    io_uring_register_files_update(this->ring_, 2, &this->rdb_fd_, 1);
    this->fixed_fds_[2] = this->rdb_fd_;

    this->rdb_is_saving_ = true;
    this->rdb_cow_bucket_cursor_ = 0;

    auto& map = this->engine_->getUnderlyingMap();
    map.max_load_factor(1000000.0);

    doRdbWriteStep();
}

void EventLoop::doRdbWriteStep() {
#if 0
    if(!this->rdb_is_saving_) return;

    auto& map = this->engine_->getUnderlyingMap();
    std::size_t total_buckets = map.bucket_count();

    std::size_t step_limit = 100;
    std::size_t processed = 0;

    while(this->rdb_cow_bucket_cursor_ < total_buckets && processed < step_limit) {
        for(auto it = map.begin(this->rdb_cow_bucket_cursor_);
            it != map.end(this->rdb_cow_bucket_cursor_); ++it) {

            if(this->rdb_cow_saved_keys_.find(it->first) == this->rdb_cow_saved_keys_.end()) {
                rkv::KvsProtocolHandler::dumpObjToBinaryRdb(&this->rdb_write_buffer_, it->first, it->second);
            }
        }

        ++this->rdb_cow_bucket_cursor_;
        ++processed;
    }

    if(this->rdb_write_buffer_.readableBytes() > 0) {
        tryFlushRdb();
    }

    if(this->rdb_cow_bucket_cursor_ >= total_buckets) {
        this->rdb_is_saving_ = false;
    }
#endif
}


void EventLoop::tryFlushRdb() {
    if(!this->rdb_is_saving_) return;

    io_uring_sqe* sqe = io_uring_get_sqe(this->ring_);
    if(!sqe) {
        io_uring_submit(this->ring_);
        sqe = io_uring_get_sqe(this->ring_);
    }

    io_uring_prep_write(sqe, 2, this->rdb_write_buffer_.peek(), 
    this->rdb_write_buffer_.readableBytes(), 0);
    sqe->flags |= IOSQE_FIXED_FILE;
    io_uring_sqe_set_data(sqe, this->rdbWrite_req_.get());
}

void EventLoop::initReq() {
    this->aof_req_ = std::make_unique<AofRequest>();
    this->aof_req_->setLoop(this);
    this->aof_req_->setType(IoType::AOF_WRITE);

    this->aoffsync_req_ = std::make_unique<AoffsyncRequest>();
    this->aoffsync_req_->setLoop(this);
    this->aoffsync_req_->setType(IoType::AOF_FSYNC);

    this->aof_timer_req_ = std::make_unique<TimerRequest>();
    this->aof_timer_req_->setLoop(this);
    this->aof_timer_req_->setType(IoType::TIMER);

    this->rdb_timer_req_ = std::make_unique<TimerRequest>();
    this->rdb_timer_req_->setLoop(this);
    this->rdb_timer_req_->setType(IoType::TIMER);

    this->handShake_timer_req_ = std::make_unique<TimerRequest>();
    this->handShake_timer_req_->setLoop(this);
    this->handShake_timer_req_->setType(IoType::TIMER);

    this->actExpire_timer_req_ = std::make_unique<TimerRequest>();
    this->actExpire_timer_req_->setLoop(this);
    this->actExpire_timer_req_->setType(IoType::TIMER);

    this->HttpConnect_timer_req_ = std::make_unique<TimerRequest>();
    this->HttpConnect_timer_req_->setLoop(this);
    this->HttpConnect_timer_req_->setType(IoType::TIMER);

    this->aofrewrite_req_ = std::make_unique<AofRewriteRequest>();
    this->aofrewrite_req_->setLoop(this);
    this->aofrewrite_req_->setType(IoType::AOF_REWRITE);

    this->rdbWrite_req_ = std::make_unique<RdbWriteRequest>();
    this->rdbWrite_req_->setLoop(this);
    this->rdbWrite_req_->setType(IoType::RDB_WRITE);

    if(!rkv::Config::getInstance().is_master_) {
        this->handShake_req_ = std::make_unique<HandShakeRequest>();
        this->handShake_req_->setLoop(this);
        this->handShake_req_->setType(IoType::HANDSHAKE);
    }
}

void EventLoop::initAof() {
    if(rkv::Config::getInstance().aof_enabled_) {
        this->aof_fd_ = ::open(this->aof_file_path_.c_str(), O_WRONLY | O_CREAT, 0644);
        if (this->aof_fd_ < 0) {
            std::cerr << "Failed to open AOF file: " << this->aof_file_path_ 
                    << ", Error: " << strerror(errno) << std::endl;

            return;
        }

        io_uring_register_files_update(this->ring_, 1, &this->aof_fd_, 1);
        this->fixed_fds_[1] = this->aof_fd_;

        if(rkv::Config::getInstance().aof_sync_type_ == rkv::AofSyncType::EVERYSEC) {
            this->addTimer(1000, [this] () {
                this->tryFsyncAof();

                this->addTimer(1000, [this] () {
                    this->tryFsyncAof();
                }, this->aof_timer_req_.get());
            }, this->aof_timer_req_.get());
        }
    }
}

void EventLoop::initRdb() {
    if(rkv::Config::getInstance().rdb_enabled_) {
        this->addTimer(rkv::Config::getInstance().rdb_interval_ * 1000, [this] () {
            this->startRdb();

            this->addTimer(rkv::Config::getInstance().rdb_interval_ * 1000, [this] () {
                this->startRdb();
            }, this->rdb_timer_req_.get());
        }, this->rdb_timer_req_.get());
    }
}

void EventLoop::startHandshake() {
    if(this->handShake_req_->fd_ > 0) {
        ::close(this->handShake_req_->fd_);
    }

    this->handShake_req_->setFd(Socket::setNoblockingSocket());
    this->handShake_req_->setAddr(rkv::Config::getInstance().master_ip_.c_str(), rkv::Config::getInstance().master_port_);
    this->handShake_req_->state_ = HandshakeState::CONNECTING;
    this->handShake_req_->write_offset_ = 0;

    io_uring_sqe* sqe = io_uring_get_sqe(this->ring_);
    if(!sqe) {
        io_uring_submit(this->ring_);
        sqe = io_uring_get_sqe(this->ring_);
    }

    io_uring_prep_connect(sqe, this->handShake_req_->fd_, (sockaddr*)&this->handShake_req_->addr_, sizeof(sockaddr_in));
    io_uring_sqe_set_data(sqe, this->handShake_req_.get());
}

void EventLoop::doHandshake() {
    this->handShake_req_->state_ = HandshakeState::WRITING;

    io_uring_sqe* sqe = io_uring_get_sqe(this->ring_);
    if(!sqe) {
        io_uring_submit(this->ring_);
        sqe = io_uring_get_sqe(this->ring_);
    }
    
    const char* msg = this->handShake_instruction_.data() + this->handShake_req_->write_offset_;
    std::size_t remain = this->handShake_instruction_.size() - this->handShake_req_->write_offset_;

    io_uring_prep_write(sqe, this->handShake_req_->fd_, msg, remain, 0);
    io_uring_sqe_set_data(sqe, this->handShake_req_.get());
}

void EventLoop::startFullSync(const TcpConnectionPtr& conn) {
    if(conn->getLoop() != this) return;

    
}

void EventLoop::addToReplicas_(const TcpConnectionPtr& conn) {
    auto it = std::find_if(this->replicas_.begin(), this->replicas_.end(), [conn] (const auto& p) {
        return p.first == conn;
    });

    if(it == this->replicas_.end()) {
        auto sendRepl_req = std::make_unique<SendReplicaRequest>();
        sendRepl_req->setConn(conn);
        sendRepl_req->setLoop(this);
        sendRepl_req->setType(IoType::REPL_SEND);

        this->replicas_.emplace_back(conn, std::move(sendRepl_req));
    }
}

void EventLoop::removeFromReplicas_(const TcpConnectionPtr& conn) {
    auto it = std::find_if(this->replicas_.begin(), this->replicas_.end(), [conn] (const auto& p) {
        return p.first == conn;
    });

    if(it != this->replicas_.end()) {
        this->replicas_.erase(it);
    }
}

void EventLoop::appendReplicationBacklog(const std::string& cmd_data) {
    this->repl_backlog_.append(cmd_data.data(), cmd_data.size());

    for(auto& [conn, req] : this->replicas_) {

        this->sendToReplica(conn, req.get());
    }
}

void EventLoop::sendToReplica(const TcpConnectionPtr& conn, SendReplicaRequest* req) {
    if(this->repl_is_writing_) return;

    SendChunk sendchunk = this->repl_backlog_.getChunkToSend(conn->getReplicaOffset());

    if(sendchunk.len_ == static_cast<std::size_t>(-1)) {
        // LOG_ERROR << ""
        return;
    }

    io_uring_sqe* sqe = io_uring_get_sqe(this->ring_);
    if(!sqe) {
        io_uring_submit(this->ring_);
        sqe = io_uring_get_sqe(this->ring_);
    }

    io_uring_prep_send(sqe, conn->getFixedIndex(), sendchunk.data_, sendchunk.len_, 0);
    sqe->flags |= IOSQE_FIXED_FILE;
    io_uring_sqe_set_data(sqe, req);
}

void EventLoop::do_Active_Expire() {
    int count{0}, timeout_count{0};

    if(rkv::kvserver::expires_.getUnderlyingMap().empty()) return;

    for(auto it = rkv::kvserver::expires_.getUnderlyingMap().begin(); it != rkv::kvserver::expires_.getUnderlyingMap().end(), 
    count <= ACTIVEEXPIRESIZE; ++count) {

        if(getCurrentTimestamp() >= it->second) {
            rkv::kvserver::expires_.getUnderlyingMap().erase(it);
            ++timeout_count;

        } else {
            ++it;
        }
    }

    while(timeout_count >= 0.25 * ACTIVEEXPIRESIZE) {
        timeout_count = 0;

        for(auto it = rkv::kvserver::expires_.getUnderlyingMap().begin(); it != rkv::kvserver::expires_.getUnderlyingMap().end(), 
        count <= ACTIVEEXPIRESIZE; ++count) {

            if(getCurrentTimestamp() >= it->second) {
                rkv::kvserver::expires_.getUnderlyingMap().erase(it);
                ++timeout_count;

            } else {
                ++it;
            }
        }
    }
}

void EventLoop::start_Active_Expire() {
    this->addTimer(1000, [this] () {
        this->do_Active_Expire();

        this->addTimer(1000, [this] () {
            this->do_Active_Expire();
        }, this->actExpire_timer_req_.get());

    }, this->actExpire_timer_req_.get());
}

ReplBacklog* EventLoop::replBacklog()
{ return &this->repl_backlog_; }

void EventLoop::setHttpConnectCallback(const HttpConnectCallback& cb)
{ this->HttpConnectCallback_ = cb; }

void EventLoop::startConnectHttp() {
    if(!this->HttpConnect_req_) {
        this->HttpConnect_req_ = std::make_unique<HttpConnectRequest>();
        this->HttpConnect_req_->setLoop(this);
        this->HttpConnect_req_->setType(IoType::HTTP_CONNECT);
    }

    if(this->HttpConnect_req_->fd_ > 0) {
        ::close(this->HttpConnect_req_->fd_);
    }

    this->HttpConnect_req_->setFd(Socket::setNoblockingSocket());
    this->HttpConnect_req_->setAddr(rkv::Config::getInstance().master_ip_.c_str(), rkv::Config::getInstance().master_port_);

    io_uring_sqe* sqe = io_uring_get_sqe(this->ring_);
    if(!sqe) {
        io_uring_submit(this->ring_);
        sqe = io_uring_get_sqe(this->ring_);
    }

    io_uring_prep_connect(sqe, this->HttpConnect_req_->fd_, (sockaddr*)&this->HttpConnect_req_->addr_, sizeof(sockaddr_in));
    io_uring_sqe_set_data(sqe, this->HttpConnect_req_.get());
}

void EventLoop::loop() {
std::cout << "loop begin" << std::endl;

    this->armWakeupFd();

    initReq();
    initAof();
    initRdb();
    start_Active_Expire();

    if(rkv::Config::getInstance().cluster_enabled_ && !rkv::Config::getInstance().is_master_) {
        startHandshake();
    }

    while(!this->quit_) {

#if 0
        int ret = io_uring_submit_and_wait(this->ring_, 1);

        if (ret < 0) {
            if (ret == -EINTR) {
                // 被操作系统的信号打断了，检查一下 quit_ 标志位，准备退出
                continue; 
            }
            // LOG_ERROR
        }

#elif 0
        io_uring_submit(this->ring_);

        while(io_uring_peek_cqe(this->ring_, &cqe) != 0) {
            __asm__ volatile("pause" ::: "memory"); 
            if (this->quit_) return;
        }

#endif

        io_uring_cqe* cqe;
        unsigned head;
        unsigned count{0};

        io_uring_for_each_cqe(this->ring_, head, cqe){

            uint64_t user_data = io_uring_cqe_get_data64(cqe);

            if(user_data == WAKEUP_MSG_DATA) {
                this->armWakeupFd();
                ++count;

                continue;
            }

            GeneralHead* req_head = static_cast<GeneralHead*>(io_uring_cqe_get_data(cqe));
            if(req_head->type_== IoType::RECV) {
                IoRequest* recvreq = reinterpret_cast<IoRequest*>(req_head);
                recvreq->onComplete(cqe->res);

            } else if(req_head->type_ == IoType::SEND) {
                ReplyBuffer* replybuf = reinterpret_cast<ReplyBuffer*>(req_head);
                replybuf->onComplete(cqe->res);

            } else if(req_head->type_ == IoType::ACCEPT) {
                IoReqeustAcceptor* acceptorreq = reinterpret_cast<IoReqeustAcceptor*>(req_head);
                acceptorreq->onComplete(cqe->res);

            } else if(req_head->type_ == IoType::CLOSE) {
                CloseRequest* closereq = reinterpret_cast<CloseRequest*>(req_head);
                closereq->onComplete();

            } else if(req_head->type_ == IoType::AOF_WRITE) {
                AofRequest* aofreq = reinterpret_cast<AofRequest*>(req_head);
                aofreq->onComplete(cqe->res);

            } else if(req_head->type_ == IoType::AOF_FSYNC) {
                AoffsyncRequest* aoffsyncreq = reinterpret_cast<AoffsyncRequest*>(req_head);
                aoffsyncreq->onComplete(cqe->res);

            } else if(req_head->type_ == IoType::TIMER) {
                TimerRequest* timerreq = reinterpret_cast<TimerRequest*>(req_head);
                timerreq->onComplete(cqe->res);

            } else if(req_head->type_ == IoType::AOF_REWRITE) {
                AofRewriteRequest* aofrewritereq = reinterpret_cast<AofRewriteRequest*>(req_head);
                aofrewritereq->onComplete(cqe->res);

            } else if(req_head->type_ == IoType::AOF_REWRITE) {
                RdbWriteRequest* rdbwritereq = reinterpret_cast<RdbWriteRequest*>(req_head);
                rdbwritereq->onComplete(cqe->res);

            } else if(req_head->type_ == IoType::HANDSHAKE) {
                HandShakeRequest* handshakereq = reinterpret_cast<HandShakeRequest*>(req_head);
                handshakereq->onComplete(cqe->res);

            } else if(req_head->type_ == IoType::REPL_SEND) {
                SendReplicaRequest* sendReplReq = reinterpret_cast<SendReplicaRequest*>(req_head);
                sendReplReq->onComplete(cqe->res);

            } else if(req_head->type_ == IoType::HTTP_SEND) {
                PendingWrite* pw = reinterpret_cast<PendingWrite*>(req_head);
                pw->onComplete(cqe->res);

            } else if(req_head->type_ == IoType::HTTP_CONNECT) {
                HttpConnectRequest* httpConnectReq = reinterpret_cast<HttpConnectRequest*>(req_head);
                httpConnectReq->onComplete(cqe->res);

            } else {
                std::cout << "CRITICAL: Unknown CQE type: " << (int)req_head->type_ 
                        << " ptr: " << req_head << std::endl;
            }

            ++count;
        };

        if(count > 0) io_uring_cq_advance(this->ring_, count);

        bool did_task = doPendingFunctors();
        tryFlushAof();
#if 1
        if(count == 0 && !did_task) {
            this->is_sleeping_.store(true, std::memory_order_seq_cst);

            if(this->pending_functors_->size_approx() > 0) {
                this->is_sleeping_.store(false, std::memory_order_seq_cst);
                io_uring_submit(this->ring_);

            } else {
                io_uring_submit_and_wait(this->ring_, 1);
                this->is_sleeping_.store(false, std::memory_order_seq_cst);
            }

        } else {
            io_uring_submit(this->ring_);
        }
#endif
    }
}

void EventLoop::quit() {
    this->quit_ = true;

    if(!isInLoopThread()) {
        wakeup();
    }
}

bool EventLoop::doPendingFunctors() {
    bool is_did_task{false};
    Functor cb;

    while(this->pending_functors_->try_dequeue(cb)) 
    { cb(); is_did_task = true; }

    return is_did_task;
}

};

};