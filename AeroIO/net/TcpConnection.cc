#include "TcpConnection.h"
#include "EventLoop.h"

#include <assert.h>
#include <liburing.h>
#include <immintrin.h>

namespace {

io_uring_sqe* get_sqe_safe(io_uring* ring) {
    io_uring_sqe* sqe = io_uring_get_sqe(ring);
    int retry_count = 0;
    while (!sqe) {
        io_uring_submit(ring);
        sqe = io_uring_get_sqe(ring);
        if (!sqe) {
            _mm_pause();
            if (++retry_count > 1000) {
                std::this_thread::yield();
                retry_count = 0;
            }
        }
    }

    return sqe;
}

};

namespace AeroIO {

namespace net {

TcpConnection::TcpConnection(EventLoop* loop, int socket, PoolContext& ctx) : 
    loop_(loop), 
    state_(StateE::kConnecting),
    socket_(std::make_unique<Socket>(socket)),
    Poolctx_(ctx), is_writing_(false) {

    // socket_->setKeepAlive(true);
}

TcpConnection::~TcpConnection()
{ assert(this->state_ == StateE::kDisconnected); }

UringBuffer* TcpConnection::getInputBuffer()
{ return &this->inputBuffer_; }

int TcpConnection::fd() const
{ return this->socket_->fd(); }

void TcpConnection::setMessageCallback(const MessageCallback& cb)
{ this->messageCallback_ = cb; }

void TcpConnection::setCloseCallback(const CloseCallback& cb)
{ this->closeCallback_ = cb; }

void TcpConnection::setConnectionCallback(const ConnectionCallback& cb)
{ this->connectionCallback_ = cb; }

void TcpConnection::setWriteCompleteCallback(const WriteCompleteCallback& cb)
{ this->writeCompleteCallback_ = cb; }

bool TcpConnection::connected() const
{ return this->state_ == StateE::kConnected; }

bool TcpConnection::disconnected() const
{ return this->state_ == StateE::kDisconnected; }

void TcpConnection::setTcpNoDelay(bool on)
{ this->socket_->setTcpNoDelay(on); }

void TcpConnection::setReusePort(bool on)
{ this->socket_->setReusePort(on); }

EventLoop* TcpConnection::getLoop() const
{ return this->loop_; }

void TcpConnection::setContext(const std::any& context)
{ this->context_ = context; }

const std::any& TcpConnection::getContext() const
{ return this->context_; }

std::any* TcpConnection::getMutableContext()
{ return &this->context_; }

void TcpConnection::setFixedFileIndex(int index)
{ this->socket_->setFixedFileIndex(index); }

int TcpConnection::getFixedIndex() const
{ return this->socket_->getFixedIndex(); }

void TcpConnection::setNoregister()
{ this->socket_->setNoregister(); }

ReplyBufferPtr TcpConnection::getCurrentReplyBuffer() {
    if(!this->current_reply_) {
        this->current_reply_ = this->Poolctx_.replyBufferPool->get();
        this->current_reply_->setConn(shared_from_this());
        this->current_reply_->in_used_ = true;
    }

    return this->current_reply_;
}

bool TcpConnection::tryFillReplyBuffer(std::string& msg) {
    if (this->disconnected()) {
        return false;
    }

    ReplyBufferPtr reply = this->getCurrentReplyBuffer();
    if (!reply) return false;

    reply->appendString(std::move(msg));

    if(reply->isNearlyFull()) {
        this->backlog_.push_back(reply);
        this->current_reply_.reset();
    }

    return true;
}

void TcpConnection::flushWriteBatch() {
    if(this->state_ == StateE::kConnected) {
        if(this->loop_->isInLoopThread()) {
            
            if(!this->is_writing_) send_start();

        } else {
            this->loop_->runInLoop([conn = shared_from_this()] () {

                conn->send_start();
            });
        }
    }
}

void TcpConnection::send_start() {
    if(this->loop_->ring() == nullptr) return;

    if(this->is_writing_) return;
    this->is_writing_ = true;

    if(!this->backlog_.empty()) {
        this->writing_reply_ = this->backlog_.front();
        this->backlog_.pop_front();

    } else if(this->current_reply_ && this->current_reply_->total_bytes_ > 0) {
        this->writing_reply_ = this->current_reply_;
        this->current_reply_.reset();

    } else {
        this->is_writing_ = false;
        return;
    }

    if(!this->connected() || this->writing_reply_->mempool_ == nullptr) {
        this->writing_reply_.reset();
        this->is_writing_ = false;
        return;
    }

    io_uring_sqe* sqe = get_sqe_safe(this->loop_->ring());
    int index = this->getFixedIndex();

    if(!sqe) {
        if(this->writing_reply_) {
            this->backlog_.push_front(this->writing_reply_);
            this->writing_reply_.reset();
        }

        this->is_writing_ = false;
        return;
    }

    if(index < 0 || this->loop_->getFixedFds()[index] < 0) {
        this->writing_reply_.reset();
        this->is_writing_ = false;
        this->forceClose();
        return;
    }

    write_in_flight_ = true;
    io_uring_prep_writev(sqe, index, this->writing_reply_->iovs_, this->writing_reply_->iov_cnt_, 0);
    sqe->flags |= IOSQE_FIXED_FILE;
    io_uring_sqe_set_data(sqe, this->writing_reply_.get());
}

void TcpConnection::send_schedul(int res_bytes) {
    write_in_flight_ = false;
    if (!connected()) { handleClose(); return; }

    if(res_bytes <= 0) {
        if(res_bytes == -EAGAIN || res_bytes == -EWOULDBLOCK) {
            io_uring_sqe* sqe = get_sqe_safe(this->loop_->ring());

            int index = this->getFixedIndex();
            if(index >= 0 && this->loop_->getFixedFds()[index] >= 0) {
                write_in_flight_ = true;
                io_uring_prep_writev(sqe, index, 
                    &this->writing_reply_->iovs_[this->writing_reply_->iov_start_idx_], 
                    this->writing_reply_->iov_cnt_ - this->writing_reply_->iov_start_idx_, 0);
                sqe->flags |= IOSQE_FIXED_FILE;
                io_uring_sqe_set_data(sqe, this->writing_reply_.get());
                return;
            }
        }

        // this->is_writing_ = false;
        forceClose();
        return;
    }

    if(!this->writing_reply_) {
        // this->is_writing_ = false;
        return;
    }

    if(res_bytes == this->writing_reply_->total_bytes_) {
        this->writing_reply_.reset();
        this->is_writing_ = false;

        if(!this->backlog_.empty() || (this->current_reply_ && this->current_reply_->total_bytes_ > 0)) {
            this->send_start();

        } else {
            this->is_writing_ = false;
        }
        
    } else if(res_bytes > 0 && res_bytes < this->writing_reply_->total_bytes_) {
        this->writing_reply_->adjustForPartialWrite(res_bytes);

        io_uring_sqe* sqe = get_sqe_safe(this->loop_->ring());

        int index = this->getFixedIndex();

        if(index < 0 || this->loop_->getFixedFds()[index] < 0) {
            this->forceClose();
            return;
        }

        write_in_flight_ = true;
        io_uring_prep_writev(sqe, index, &this->writing_reply_->iovs_[this->writing_reply_->iov_start_idx_], 
        this->writing_reply_->iov_cnt_ - this->writing_reply_->iov_start_idx_, 0);
        sqe->flags |= IOSQE_FIXED_FILE;
        io_uring_sqe_set_data(sqe, this->writing_reply_.get());
    }
}

void TcpConnection::start() {
    this->read_req_ = std::make_unique<IoRequest>(shared_from_this());
    read_req_->setType(IoType::RECV);


    io_uring_sqe* sqe = get_sqe_safe(this->loop_->ring());

    IoRequest* req = this->read_req_.get();
    req->setConn(shared_from_this());

    int index = this->getFixedIndex();
    
    if(index < 0 || this->loop_->getFixedFds()[index] < 0) {
        this->forceClose();
        return;
    }

    this->active_recv_block_ = this->Poolctx_.blockPool->get();
    this->recv_blocks_.push_back(this->active_recv_block_);

    read_in_flight_ = true;
    io_uring_prep_recv(sqe, index, (void*)this->active_recv_block_->beginWrite(), 
    this->active_recv_block_->writableBytes(), 0);
    sqe->flags |= IOSQE_FIXED_FILE;
    io_uring_sqe_set_data(sqe, req);
}

void TcpConnection::handleRead(int res_bytes) {
    read_in_flight_ = false;
    if (!connected()) { handleClose(); return; }

    if(res_bytes > 0) {
        this->active_recv_block_->hasWritten(res_bytes);

        if(this->messageCallback_) {
            this->messageCallback_(shared_from_this(), this->recv_blocks_);
        }

    } else if(res_bytes == 0 || (res_bytes < 0 && res_bytes != -EAGAIN)) {
        onPostClosereq();
        return;
    }

    if(this->loop_ == nullptr) return;

    while(!this->recv_blocks_.empty()) {
        if(this->recv_blocks_.front()->readableBytes() == 0) {
            this->recv_blocks_.pop_front();

        } else break;
    }

    if(this->recv_blocks_.empty() || this->recv_blocks_.back()->writableBytes() < 512) {
        this->active_recv_block_ = this->Poolctx_.blockPool->get();
        this->recv_blocks_.push_back(this->active_recv_block_);

    } else {
        this->active_recv_block_ = this->recv_blocks_.back();
    }

    io_uring_sqe* sqe = get_sqe_safe(this->loop_->ring());

    IoRequest* req = this->read_req_.get();
    req->setConn(shared_from_this());

    int index = this->getFixedIndex();
    if(index < 0 || this->loop_->getFixedFds()[index] < 0) {
        this->forceClose();
        return;
    }

    read_in_flight_ = true;
    io_uring_prep_recv(sqe, index, (void*)this->active_recv_block_->beginWrite(), 
    this->active_recv_block_->writableBytes(), 0);
    sqe->flags |= IOSQE_FIXED_FILE;
    io_uring_sqe_set_data(sqe, req);
}



void TcpConnection::handleClose() {
    if (disconnected() || read_in_flight_ || write_in_flight_) return;
    auto keep_alive = shared_from_this();
    const int index = getFixedIndex();
    if (index >= 0) {
        int empty = -1;
        io_uring_register_files_update(loop_->ring(), index, &empty, 1);
        loop_->queueInfreeFixedFds(index);
        loop_->getFixedFds()[index] = -1;
        setNoregister();
    }
    state_ = StateE::kDisconnected;
    // The callback removes the connection using its original fd.
    if (closeCallback_) closeCallback_(keep_alive);
    socket_->close();
    context_.reset();
    backlog_.clear();
    current_reply_.reset();
    writing_reply_.reset();
    recv_blocks_.clear();
    active_recv_block_.reset();
}

void TcpConnection::onPostClosereq() { forceClose(); }

void TcpConnection::stopAfterIoCancelled() {
    state_ = StateE::kDisconnected;
    read_in_flight_ = false;
    write_in_flight_ = false;
    socket_->close();
    context_.reset();
    backlog_.clear();
    current_reply_.reset();
    writing_reply_.reset();
    recv_blocks_.clear();
    active_recv_block_.reset();
}

void TcpConnection::handleError() {

}

void TcpConnection::connectEstablished() {
    this->state_ = StateE::kConnected;

    if(this->connectionCallback_) {
        this->connectionCallback_(shared_from_this());
    }
}

void TcpConnection::connectDestroyed() {
    this->state_ = StateE::kDisconnected;

    if(this->connectionCallback_) {
        this->connectionCallback_(shared_from_this());
    }
}

void TcpConnection::forceClose() {
    if (disconnected()) return;
    state_ = StateE::kDisconnecting;
    // Keep request/buffer ownership until both completions have arrived.
    ::shutdown(fd(), SHUT_RDWR);
    handleClose();
}

void TcpConnection::send(std::string data) {
    if (!loop_->isInLoopThread()) {
        loop_->runInLoop([self = shared_from_this(), data = std::move(data)]() mutable {
            self->send(std::move(data));
        });
        return;
    }
    if (!connected()) return;
    if (tryFillReplyBuffer(data)) flushWriteBatch();
}

}
}
