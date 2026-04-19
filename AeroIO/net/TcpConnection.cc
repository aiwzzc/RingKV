#include "TcpConnection.h"
#include "EventLoop.h"
#include "TcpServer.h"
#include "src/common.h"

#include <string.h>
#include <assert.h>
#include <liburing.h>
#include <iostream>
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

TcpConnection::TcpConnection(EventLoop* loop, int socket, rkv::ServerContext* ctx) : 
    loop_(loop), state_(StateE::kConnecting), socket_(std::make_unique<Socket>(socket)),
    Serverctx_(ctx), is_replica_(false), need_reply_(true),
    is_writing_(false) {

    // socket_->setKeepAlive(true);
    this->send_pending_ = std::make_unique<PendingWrite>(this->Serverctx_->mempool);
    this->send_pending_->setType(IoType::HTTP_SEND);
}

TcpConnection::~TcpConnection()
{ assert(this->state_ == StateE::kDisconnected); }

UringBuffer* TcpConnection::getInputBuffer()
{ return &this->inputBuffer_; }

int TcpConnection::fd() const
{ return this->socket_->fd(); }

ConnState TcpConnection::getConnState()
{ return this->connState_; }

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

void TcpConnection::setConnState(ConnState state)
{ this->connState_ = state; }

std::vector<RouteBatch>& TcpConnection::route_batches()
{ return this->route_batches_; }

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

uint64_t TcpConnection::appendPendRes(ResponseSlot& slot) {
    slot.id_ = this->next_slot_id_++;
    this->pending_responses_.push_back(std::move(slot));

    return slot.id_;
}

int TcpConnection::pendResIndex()
{ return this->pending_responses_.size(); }

ReplyBufferPtr TcpConnection::getCurrentReplyBuffer() {
    if(!this->current_reply_) {
        this->current_reply_ = this->Serverctx_->replyBufPool->get();
        this->current_reply_->setConn(shared_from_this());
        this->current_reply_->in_used_ = true;
    }

    return this->current_reply_;
}

void TcpConnection::fillSingleSlot(uint64_t slot_id, std::string&& data) {
    if(!this->connected() || slot_id == 0) return;

    uint64_t front_id = this->pending_responses_.front().id_;

    if(slot_id >= front_id && (slot_id - front_id) < this->pending_responses_.size()) {
        auto& slot = this->pending_responses_[slot_id - front_id];
        slot.data_ = std::move(data);
        slot.is_ready_ = true;
    }
}

void TcpConnection::fillPendingSlots(std::vector<std::pair<std::string, uint64_t>>&& result) {
    if (!this->connected()) { return; }

    for(auto& res : result) {
        fillSingleSlot(res.second, std::move(res.first));
    }
}

void TcpConnection::tryFlushResponses() {
    if (!this->connected()) { return; }

    ReplyBufferPtr reply = this->getCurrentReplyBuffer();
    if (!reply) return;

    while(!this->pending_responses_.empty()) {
        auto& slot = this->pending_responses_.front();

        if(!slot.is_ready_) break;

        // reply->appendStatic(slot.data_.data(), slot.data_.size());
        reply->appendString(std::move(slot.data_));
        this->pending_responses_.pop_front();

        if(reply->isNearlyFull()) {
            if(this->is_writing_) {
                this->backlog_.push_back(reply);
                this->current_reply_.reset();

            } else {
                this->flushWriteBatch();
            }

            if (!this->connected()) {
                return;
            }

            if (!this->connected()) return;
            reply = this->getCurrentReplyBuffer();
            if (!reply) break;
        }
    }

    if(this->current_reply_ && this->current_reply_->total_bytes_ > 0) {
        if(!this->is_writing_) {
            this->flushWriteBatch();
        }
    }
}

void TcpConnection::start() {
    this->read_req_ = std::make_unique<IoRequest>(shared_from_this());
    read_req_->setType(IoType::RECV);

    this->close_req_ = std::make_unique<CloseRequest>(shared_from_this());
    close_req_->setType(IoType::CLOSE);

    // io_uring_sqe* sqe = io_uring_get_sqe(this->loop_->ring());
    // if(!sqe) {
    //     io_uring_submit(this->loop_->ring());

    //     sqe = io_uring_get_sqe(this->loop_->ring());
    // }

    io_uring_sqe* sqe = get_sqe_safe(this->loop_->ring());

    IoRequest* req = this->read_req_.get();
    req->setConn(shared_from_this());

    int index = this->getFixedIndex();
    
    if(index < 0 || this->loop_->getFixedFds()[index] < 0) {
        this->forceClose();
        return;
    }

    this->active_recv_block_ = this->Serverctx_->blockPool->get();
    this->recv_blocks_.push_back(this->active_recv_block_);

    io_uring_prep_recv(sqe, index, (void*)this->active_recv_block_->beginWrite(), 
    this->active_recv_block_->writableBytes(), 0);
    sqe->flags |= IOSQE_FIXED_FILE;
    io_uring_sqe_set_data(sqe, req);
}

void TcpConnection::handleRead(int res_bytes) {
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
        this->active_recv_block_ = this->Serverctx_->blockPool->get();
        this->recv_blocks_.push_back(this->active_recv_block_);

    } else {
        this->active_recv_block_ = this->recv_blocks_.back();
    }

    // io_uring_sqe* sqe = io_uring_get_sqe(this->loop_->ring());
    // if(!sqe) {
    //     io_uring_submit(this->loop_->ring());

    //     sqe = io_uring_get_sqe(this->loop_->ring());
    // }
    io_uring_sqe* sqe = get_sqe_safe(this->loop_->ring());

    IoRequest* req = this->read_req_.get();
    req->setConn(shared_from_this());

    int index = this->getFixedIndex();
    if(index < 0 || this->loop_->getFixedFds()[index] < 0) {
        this->forceClose();
        return;
    }

    io_uring_prep_recv(sqe, index, (void*)this->active_recv_block_->beginWrite(), 
    this->active_recv_block_->writableBytes(), 0);
    sqe->flags |= IOSQE_FIXED_FILE;
    io_uring_sqe_set_data(sqe, req);
}

#if 0
void TcpConnection::handleRead(int res_bytes) {
    if(res_bytes > 0) {
        this->inputBuffer_.hasWritten(res_bytes);

        if(this->messageCallback_) {
            this->messageCallback_(shared_from_this(), &this->inputBuffer_);
        }

    } else if(res_bytes == 0 || (res_bytes < 0 && res_bytes != -EAGAIN)) {
        onPostClosereq();
        return;
    }

    if(this->loop_ == nullptr) return;

    if(this->inputBuffer_.writableBytes() < UringBuffer::kMinWritableBytes) {
        this->inputBuffer_.EnsureFreeSpace(UringBuffer::kMinWritableBytes);
    }

    io_uring_sqe* sqe = io_uring_get_sqe(this->loop_->ring());
    if(!sqe) {
        io_uring_submit(this->loop_->ring());

        sqe = io_uring_get_sqe(this->loop_->ring());
    }

    IoRequest* req = this->read_req_.get();
    req->setConn(shared_from_this());

    int index = this->getFixedIndex();
    if(index < 0 || this->loop_->getFixedFds()[index] < 0) {
        this->forceClose();
        return;
    }

    io_uring_prep_recv(sqe, index, (void*)this->inputBuffer_.beginWrite(), this->inputBuffer_.writableBytes(), 0);
    sqe->flags |= IOSQE_FIXED_FILE;
    io_uring_sqe_set_data(sqe, req);
}
#endif

void TcpConnection::handleClose() {
    if(this->state_ == StateE::kDisconnected) return;

    TcpConnectionPtr guardThis(shared_from_this());

    if(this->closeCallback_) {
        this->closeCallback_(guardThis);
    }

    if(this->current_reply_) {
        // this->replyBufPool_->release(this->current_reply_);
        this->current_reply_.reset();
    }

    this->pending_responses_.clear();

    if(int index = getFixedIndex()) {
        int remove_fd = -1;
        io_uring_register_files_update(this->loop_->ring(), index, &remove_fd, 1);

        this->loop_->queueInfreeFixedFds(index);
        this->loop_->getFixedFds()[index] = -1;
        setNoregister();
    }

    this->state_ = StateE::kDisconnected;
}

void TcpConnection::onPostClosereq() {
    if(this->state_ == StateE::kDisconnected) return;

    io_uring_sqe* sqe = io_uring_get_sqe(this->loop_->ring());
    if(!sqe) {
        io_uring_submit(this->loop_->ring());
        sqe = io_uring_get_sqe(this->loop_->ring());
    }

    io_uring_prep_close(sqe, this->fd());
    io_uring_sqe_set_data(sqe, this->close_req_.get());
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

void TcpConnection::handleWrite() {
    if(this->writeCompleteCallback_) {
        this->writeCompleteCallback_(shared_from_this());
    }

    if(this->backlog_.empty()) {
        if(this->state_ == StateE::kDisconnecting) {
            this->state_ = StateE::kDisconnected;

            if(int index = getFixedIndex()) {
                int remove_fd = -1;
                io_uring_register_files_update(this->loop_->ring(), index, &remove_fd, 1);

                this->loop_->queueInfreeFixedFds(index);
                this->loop_->getFixedFds()[index] = -1;
                setNoregister();
            }

            ::shutdown(fd(), SHUT_WR);
        }

    } else {
        flush_backlog();
    }
}

void TcpConnection::detachFromLoop() {
    this->loop_ = nullptr;
    // this->read_req_.reset();
    // this->close_req_.reset();
    if(this->current_reply_) {
        // this->replyBufPool_->release(this->current_reply_);
        this->current_reply_.reset();
    }

    this->Serverctx_ = nullptr;
}

void TcpConnection::attachToLoop(EventLoop* newLoop) {
    this->loop_ = newLoop;

    this->read_req_ = std::make_unique<IoRequest>(shared_from_this());
    read_req_->setType(IoType::RECV);
    this->close_req_ = std::make_unique<CloseRequest>(shared_from_this());
    close_req_->setType(IoType::CLOSE);

    TcpServer* newTcpServer = newLoop->getTcpServer();
    this->Serverctx_ = newTcpServer->getServerContext();

    // this->

    int index = newLoop->getFreeFixedFd();

    if(index < 0) {
        index = newLoop->getNextIndex();
        if(index < 0) {
            // LOG_ERROR MAXCONNLIMIT
            return;
        }
    }

    int connfd = this->fd();
    newLoop->getFixedFds()[index] = connfd;
    
    io_uring_register_files_update(newLoop->ring(), index, &connfd, 1);

    this->setFixedFileIndex(index);

    this->setMessageCallback(newTcpServer->messageCallback_);
    this->setConnectionCallback(newTcpServer->connectionCallback_);
    this->setWriteCompleteCallback(newTcpServer->writeCompleteCallback_);
    this->setCloseCallback([newLoop, newTcpServer, this] (const TcpConnectionPtr& conn) {
        newLoop->runInLoop([newTcpServer, conn = shared_from_this()] () {
            newTcpServer->removeConnection(conn->fd());
            conn->connectDestroyed();
        });
    });

    newLoop->getTcpServer()->addConnections(connfd, shared_from_this());

    newLoop->runInLoop([conn = shared_from_this()] () {
        conn->start();
    });
}

void TcpConnection::send(const char* data, std::size_t len) {
    if(!this->connected()) return;

    this->appendToPendingWrite(data, len);

    // io_uring_sqe* sqe = io_uring_get_sqe(this->loop_->ring());
    // if(!sqe) {
    //     io_uring_submit(this->loop_->ring());
    //     sqe = io_uring_get_sqe(this->loop_->ring());
    // }

    io_uring_sqe* sqe = get_sqe_safe(this->loop_->ring());

    int index = this->getFixedIndex();
    
    if(!sqe || index < 0 || this->loop_->getFixedFds()[index] < 0) {
        this->writing_reply_.reset(); 
        this->forceClose();
        return;
    }

    io_uring_prep_writev(sqe, index, this->send_pending_->iovs_, this->send_pending_->iov_cnt_, 0);
    sqe->flags |= IOSQE_FIXED_FILE;
    io_uring_sqe_set_data(sqe, this->send_pending_.get());
}

void TcpConnection::send() {
    if(!this->connected()) return;

    // io_uring_sqe* sqe = io_uring_get_sqe(this->loop_->ring());
    // if(!sqe) {
    //     io_uring_submit(this->loop_->ring());
    //     sqe = io_uring_get_sqe(this->loop_->ring());
    // }

    io_uring_sqe* sqe = get_sqe_safe(this->loop_->ring());

    int index = this->getFixedIndex();
    
    if(!sqe || index < 0 || this->loop_->getFixedFds()[index] < 0) {
        this->writing_reply_.reset(); 
        this->forceClose();
        return;
    }

    io_uring_prep_writev(sqe, index, &this->send_pending_->iovs_[this->send_pending_->iov_start_idx_],
    this->send_pending_->iov_cnt_ - this->send_pending_->iov_start_idx_, 0);
    sqe->flags |= IOSQE_FIXED_FILE;
    io_uring_sqe_set_data(sqe, this->send_pending_.get());
}

void TcpConnection::appendToPendingWrite(const char* data, std::size_t len) {
    this->send_pending_->append(data, len);
}

void TcpConnection::setIsReplica(bool yes)
{ this->is_replica_ = yes; }

bool TcpConnection::IsReplica()
{ return this->is_replica_; }

void TcpConnection::setNeedReply(bool yes)
{ this->need_reply_ = yes; }

bool TcpConnection::NeedReply()
{ return this->need_reply_; }

std::size_t TcpConnection::getReplicaOffset() const
{ return this->replica_offset_; }

void TcpConnection::addReplOffset(std::size_t offset)
{ this->replica_offset_ += offset; }

void TcpConnection::shutdown() {
    if(this->state_ == StateE::kConnected) {
        this->state_ = StateE::kDisconnecting;

        if(this->backlog_.empty()) {
            if(int index = getFixedIndex()) {
                int remove_fd = -1;
                io_uring_register_files_update(this->loop_->ring(), index, &remove_fd, 1);

                this->loop_->queueInfreeFixedFds(index);
                this->loop_->getFixedFds()[index] = -1;
                setNoregister();
            }

            ::shutdown(fd(), SHUT_WR);
        }
    }
}

void TcpConnection::forceClose() {
    if(this->state_ == StateE::kConnected || this->state_ == StateE::kDisconnecting) {
        this->state_ = StateE::kDisconnecting;

        for(auto reply : this->backlog_) {
            // this->replyBufPool_->release(reply);
            reply.reset();
        }

        this->backlog_.clear();

        if(int index = getFixedIndex()) {
            int remove_fd = -1;
            io_uring_register_files_update(this->loop_->ring(), index, &remove_fd, 1);

            this->loop_->queueInfreeFixedFds(index);
            this->loop_->getFixedFds()[index] = -1;
            setNoregister();
        }

        ::shutdown(fd(), SHUT_RDWR);
    }
}

void TcpConnection::flushWriteBatch() {
    if(this->state_ == StateE::kConnected) {
        if(this->loop_->isInLoopThread()) {
            
            send_start();

        } else {
            this->loop_->runInLoop([conn = shared_from_this()] () {

                conn->send_start();
            });
        }
    }
}

void TcpConnection::send_start() {
    if(this->loop_->ring() == nullptr) return;

    if(!this->backlog_.empty()) {
        this->writing_reply_ = this->backlog_.front();
        this->backlog_.pop_front();

    } else if(this->current_reply_ && this->current_reply_->total_bytes_ > 0) {
        this->writing_reply_ = this->current_reply_;
        this->current_reply_.reset();

    } else {
        return;
    }

    if(!this->connected() || this->writing_reply_->mempool_ == nullptr) {
        this->writing_reply_.reset();
        return;
    }

    this->is_writing_ = true;

    // io_uring_sqe* sqe = io_uring_get_sqe(this->loop_->ring());
    // if(!sqe) {
    //     io_uring_submit(this->loop_->ring());
    //     sqe = io_uring_get_sqe(this->loop_->ring());
    // }

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
        this->forceClose();
        return;
    }

    // GeneralHead* head_ptr = static_cast<GeneralHead*>(this->writing_reply_.get());
    
    io_uring_prep_writev(sqe, index, this->writing_reply_->iovs_, this->writing_reply_->iov_cnt_, 0);
    sqe->flags |= IOSQE_FIXED_FILE;
    io_uring_sqe_set_data(sqe, this->writing_reply_.get());
}

void TcpConnection::send_schedul(int res_bytes) {
    if(res_bytes <= 0) {
        if(res_bytes == -EAGAIN || res_bytes == -EWOULDBLOCK) {
            // io_uring_sqe* sqe = io_uring_get_sqe(this->loop_->ring());
            // if(!sqe) {
            //     io_uring_submit(this->loop_->ring());
            //     sqe = io_uring_get_sqe(this->loop_->ring());
            // }

            io_uring_sqe* sqe = get_sqe_safe(this->loop_->ring());

            int index = this->getFixedIndex();
            if(index >= 0 && this->loop_->getFixedFds()[index] >= 0) {
                io_uring_prep_writev(sqe, index, 
                    &this->writing_reply_->iovs_[this->writing_reply_->iov_start_idx_], 
                    this->writing_reply_->iov_cnt_ - this->writing_reply_->iov_start_idx_, 0);
                sqe->flags |= IOSQE_FIXED_FILE;
                io_uring_sqe_set_data(sqe, this->writing_reply_.get());
                return;
            }
        }

        forceClose();
        return;
    }

    if(!this->writing_reply_) return;

    if(res_bytes == this->writing_reply_->total_bytes_) {
        this->writing_reply_.reset();
        this->is_writing_ = false;

        if(!this->backlog_.empty() || (this->current_reply_ && this->current_reply_->total_bytes_ > 0)) {
            this->send_start();
        }
        
    } else if(res_bytes > 0 && res_bytes < this->writing_reply_->total_bytes_) {
        this->writing_reply_->adjustForPartialWrite(res_bytes);

        // io_uring_sqe* sqe = io_uring_get_sqe(this->loop_->ring());
        // if(!sqe) {
        //     io_uring_submit(this->loop_->ring());
        //     sqe = io_uring_get_sqe(this->loop_->ring());
        // }

        io_uring_sqe* sqe = get_sqe_safe(this->loop_->ring());

        int index = this->getFixedIndex();

        if(index < 0 || this->loop_->getFixedFds()[index] < 0) {
            this->forceClose();
            return;
        }

        io_uring_prep_writev(sqe, index, &this->writing_reply_->iovs_[this->writing_reply_->iov_start_idx_], 
        this->writing_reply_->iov_cnt_ - this->writing_reply_->iov_start_idx_, 0);
        sqe->flags |= IOSQE_FIXED_FILE;
        io_uring_sqe_set_data(sqe, this->writing_reply_.get());
    }
}

void TcpConnection::flush_backlog() {
    if(this->backlog_.empty()) return;

    io_uring_sqe* sqe = io_uring_get_sqe(this->loop_->ring());
    if(!sqe) {
        io_uring_submit(this->loop_->ring());
        sqe = io_uring_get_sqe(this->loop_->ring());
    }

    this->writing_reply_ = this->backlog_.front();
    this->backlog_.pop_front();

    int index = this->getFixedIndex();

    if(index < 0 || this->loop_->getFixedFds()[index] < 0) {
        this->forceClose();
        return;
    }

    io_uring_prep_writev(sqe, index, this->writing_reply_->iovs_, this->writing_reply_->iov_cnt_, 0);
    sqe->flags |= IOSQE_FIXED_FILE;
    io_uring_sqe_set_data(sqe, this->writing_reply_.get());

    this->is_writing_ = true;
}

#if 0
void TcpConnection::send(const void* message, int len) {
    return send(std::string_view(static_cast<const char*>(message), len));
}

void TcpConnection::send(std::string_view message) {
    if(this->state_ == StateE::kConnected) {
        if(this->loop_->isInLoopThread()) {
            PendingWrite* pw = this->pwPool_->get();
            pw->head_.type_ = IoType::SEND;
            pw->data_size_ = message.size();

            if(message.size() < PENDINGWRITEDATASIZE) {
                ::memcpy(pw->stack_data_, message.data(), message.size());
                pw->in_heap_ = false;

            } else {
                pw->heap_data_ = std::make_shared<std::string>(message);
                pw->in_heap_ = true;
            }

            pw->in_used_ = true;
            pw->conn_ = shared_from_this();

            send_schedul(pw);

        } else {
            std::string msg_copy{message};

            this->loop_->runInLoop([this, msg = std::move(msg_copy)] () {
                PendingWrite* pw = this->pwPool_->get();
                pw->head_.type_ = IoType::SEND;
                pw->data_size_ = msg.size();

                if(msg.size() < PENDINGWRITEDATASIZE) {
                    ::memcpy(pw->stack_data_, msg.data(), msg.size());
                    pw->in_heap_ = false;

                } else {
                    pw->heap_data_ = std::make_shared<std::string>(msg);
                    pw->in_heap_ = true;
                }

                pw->in_used_ = true;
                pw->conn_ = shared_from_this();

                send_schedul(pw);
            });
        }
    }
}

void TcpConnection::send(UringBuffer* message) {
    if(this->state_ == StateE::kConnected) {
        if(this->loop_->isInLoopThread()) {
            PendingWrite* pw = this->pwPool_->get();
            pw->head_.type_ = IoType::SEND;
            pw->data_size_ = message->readableBytes();

            if(message->readableBytes() < PENDINGWRITEDATASIZE) {
                ::memcpy(pw->stack_data_, message->peek(), message->readableBytes());
                pw->in_heap_ = false;
                message->retrieveAll();

            } else {
                pw->zero_copy_buffer_ = message->takeBuffer();
                pw->in_heap_ = true;
            }

            pw->in_used_ = true;
            pw->conn_ = shared_from_this();

            send_schedul(pw);
        }
    }
}

void TcpConnection::send_schedul(PendingWrite* pw) {
    if(this->loop_->ring() == nullptr) return;

    if((pw->in_heap_ && !pw->heap_data_) || 
        (!pw->in_heap_ && !pw->stack_data_) || !pw->conn_) {
        this->pwPool_->release(pw);
        return;
    }

    if(!pw->conn_->connected()) {
        this->pwPool_->release(pw);
        return;
    }

    std::size_t remain = pw->data_size_ - pw->offset_;

    if(remain > 0) {
        io_uring_sqe* sqe = io_uring_get_sqe(this->loop_->ring());
        if(!sqe) {
            this->backlog_.push_back(pw);
            io_uring_submit(this->loop_->ring());

            return;
        }

        const char* msg = nullptr;
        if(pw->in_heap_) {
            msg = pw->heap_data_->data() + pw->offset_;

        } else {
            msg = pw->stack_data_ + pw->offset_;
        }

        io_uring_prep_send(sqe, pw->conn_->fd(), (const void*)msg, remain, MSG_NOSIGNAL);
        io_uring_sqe_set_data(sqe, pw);

    } else {
        handleWrite();
        this->pwPool_->release(pw);
    }
}

void TcpConnection::flush_backlog() {
    if(this->backlog_.empty()) return;

    while(!this->backlog_.empty()) {
        io_uring_sqe* sqe = io_uring_get_sqe(this->loop_->ring());
        if(!sqe) break;

        PendingWrite* pw = this->backlog_.front();
        this->backlog_.pop_front();

        if((pw->in_heap_ && !pw->heap_data_) || 
            (!pw->in_heap_ && !pw->stack_data_) || !pw->conn_) {
            this->pwPool_->release(pw);
            return;
        }

        std::size_t remain = pw->data_size_ - pw->offset_;
        const char* msg = nullptr;
        if(pw->in_heap_) {
            msg = pw->heap_data_->data() + pw->offset_;

        } else {
            msg = pw->stack_data_ + pw->offset_;
        }

        io_uring_prep_send(sqe, pw->conn_->fd(), (const void*)msg, remain, MSG_NOSIGNAL);
        io_uring_sqe_set_data(sqe, pw);

    }
}

#endif

};

};