#include "IoRequest.h"
#include "TcpConnection.h"
#include "EventLoop.h"
#include "TcpServer.h"
#include "http/HttpServer.h"
#include <iostream>
#include <cstring>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

namespace AeroIO {

namespace net {

IoRequest::IoRequest(TcpConnectionPtr conn) : conn_(conn) {};

IoRequest::~IoRequest() = default;

void IoRequest::setType(IoType type)
{ this->head_.type_ = type; }

void IoRequest::setConn(TcpConnectionPtr conn)
{ this->conn_ = conn; }

void IoRequest::onComplete(int res_bytes) {

    auto conn = this->conn_.lock();
    if(conn && this->head_.type_ == IoType::RECV) {
        conn->handleRead(res_bytes);
    }
}

CloseRequest::CloseRequest(TcpConnectionPtr conn) : conn_(conn) {}
CloseRequest::~CloseRequest() = default;

void CloseRequest::setType(IoType type)
{ this->head_.type_ = type; }

void CloseRequest::onComplete() {

    auto conn = this->conn_.lock();
    if(conn && this->head_.type_ == IoType::CLOSE) {
        conn->handleClose();
    }
    
}

AofRequest::AofRequest()
{}

AofRequest::~AofRequest() = default;

void AofRequest::onComplete(int res_bytes) {

    if(this->loop_ && this->head_.type_ == IoType::AOF_WRITE) {

        if(res_bytes > 0) {
            this->loop_->aof_file_offset_ += res_bytes;
            this->loop_->aof_flush_buffer_.retrieve(res_bytes);

        } else if(res_bytes < 0) {
            int err = -res_bytes;
            std::cerr << "[AOF WRITE ERROR] Failed to write AOF: " << strerror(err) << std::endl;

            // 如果是磁盘满了,应该放缓写入请求
        }

        this->loop_->aof_is_flushing_ = false;

        if(this->loop_->aof_file_offset_ >= AOFREWRITETHRESHOLD) {
            this->loop_->startAofRewrite();
        }
    }
}

void AofRequest::setType(IoType type)
{ this->head_.type_ = type; }

void AofRequest::setLoop(EventLoop* loop)
{ this->loop_ = loop; }

void AoffsyncRequest::setType(IoType type)
{ this->head_.type_ = IoType::AOF_FSYNC; }

void AoffsyncRequest::setLoop(EventLoop* loop)
{ this->loop_ = loop; }

void AoffsyncRequest::onComplete(int res_bytes) {
    
    if(this->loop_ && this->head_.type_ == IoType::AOF_FSYNC) {
        this->loop_->aof_fsync_in_flight_ = false;
        if(res_bytes >= 0) {
            this->loop_->aof_fsync_error_count_ = 0;

        } else {
            int err = -res_bytes;
            ++this->loop_->aof_fsync_error_count_;

            if(this->loop_->aof_fsync_error_count_ >= 5) {
                std::cerr << "Disk might be dead or full. Stopping writes to protect data consistency!" << std::endl;

                // 熔断机制: 向上层报警，将该KV标记为(Read Only)
                // this->kv_store_->setReadOnly(true); 
            }
        }
    }
}

void AofRewriteRequest::setType(IoType type)
{ this->head_.type_ = IoType::AOF_REWRITE; }

void AofRewriteRequest::setLoop(EventLoop* loop)
{ this->loop_ = loop; }

void AofRewriteRequest::onComplete(int res_bytes) {

    if(this->loop_ && this->head_.type_ == IoType::AOF_REWRITE) {
        if(res_bytes > 0) {
            this->loop_->aof_rewrite_flush_buffer_.retrieve(res_bytes);
            this->loop_->temp_aof_file_offset_ += res_bytes;

        } else if(res_bytes < 0) {
            int err = -res_bytes;
            std::cerr << "[AOF WRITE ERROR] Failed to write TEMP AOF: " << strerror(err) << std::endl;
        }

        if(!this->loop_->aof_is_rewriting_ && this->loop_->aof_rewrite_flush_buffer_.readableBytes() == 0) {
            this->loop_->finishAofRewrite();

        } else {
            this->loop_->doAofRewriteStep();
        }
    }

}

TimerRequest::TimerRequest() 
{}

void TimerRequest::setTimer(uint64_t ms, std::function<void()> cb) {
    this->callback_ = cb;
    ts_.tv_sec = ms / 1000;
    ts_.tv_nsec = (ms % 1000) * 1000000;
}

void TimerRequest::setLoop(EventLoop* loop)
{ this->loop_ = loop; }

void TimerRequest::setType(IoType type)
{ this->head_.type_ = type; }

void TimerRequest::onComplete(int res_bytes) {

    if(this->loop_ && this->head_.type_ == IoType::TIMER) {
        if(res_bytes == 0 || res_bytes == -ETIME) {
            if(this->callback_) callback_();
        }
    }
}

void RdbWriteRequest::setType(IoType type)
{ this->head_.type_ = type; }

void RdbWriteRequest::setLoop(EventLoop* loop)
{ this->loop_ = loop; }

void RdbWriteRequest::onComplete(int res_bytes) {

    if(this->loop_ && this->head_.type_ == IoType::RDB_WRITE) {
        if(res_bytes > 0) {
            this->loop_->rdb_write_buffer_.retrieve(res_bytes);

        } else if(res_bytes < 0) {
            int err = -res_bytes;
            std::cerr << "[AOF WRITE ERROR] Failed to write TEMP AOF: " << strerror(err) << std::endl;
        }

        if(!this->loop_->rdb_is_saving_ && this->loop_->rdb_write_buffer_.readableBytes() == 0) {
            this->loop_->rdb_write_buffer_.retrieveAll();
            this->loop_->rdb_cow_saved_keys_.clear();
            ::close(this->loop_->rdb_fd_);

        } else {
            this->loop_->doRdbWriteStep();
        }
    }
}

HandShakeRequest::~HandShakeRequest()
{ ::close(this->fd_); }

void HandShakeRequest::setType(IoType type)
{ this->head_.type_ = type; }

void HandShakeRequest::setFd(int fd)
{ this->fd_ = fd; }

void HandShakeRequest::setAddr(const char* ip, int port) {
    this->addr_.sin_family = AF_INET;
    this->addr_.sin_port = htons(port);
    this->addr_.sin_addr.s_addr = ::inet_addr(ip);
}

void HandShakeRequest::setLoop(EventLoop* loop)
{ this->loop_ = loop; }

void HandShakeRequest::reset() {
    if(this->fd_ > 0) {
        ::close(this->fd_);
        this->fd_ = -1;
    }

    this->write_offset_ = 0;
    this->error_count_ = 0;
    this->state_ = HandshakeState::CONNECTING;
}

void HandShakeRequest::onComplete(int res_bytes) {

    if(this->loop_ && this->head_.type_ == IoType::HANDSHAKE) {
        if(res_bytes < 0) {
            int err = -res_bytes;
            std::cerr << "[Handshake Error] Failed in state " 
                    << (int)this->state_ << ", error: " << strerror(err) << std::endl;

            if(++this->error_count_ >= 5) {
                std::cerr << "[Handshake Fatal] Max retries reached. Giving up." << std::endl;
                this->reset();

                return;
            }

            if(this->fd_ >= 0) {
                ::close(this->fd_);
                this->fd_ = -1;
            }

            this->loop_->addTimer(5000, [this] () {
                this->loop_->startHandshake();
            }, this->loop_->handShake_timer_req_.get());

        } else {
            if(this->state_ == HandshakeState::CONNECTING) {
                std::cout << "[Handshake] Connect success, starting write..." << std::endl;

                this->loop_->doHandshake();

            } else if(this->state_ == HandshakeState::WRITING) {
                std::cout << "[Handshake] Instruction sent completely, upgrading to TcpConnection..." << std::endl;

                this->write_offset_ += res_bytes;

                if(this->write_offset_ == this->loop_->handShake_instruction_.size()) {
                    this->state_ = HandshakeState::DONE;

                    int targer_fd = this->fd_;
                    this->fd_ = 0;

                    TcpConnectionPtr conn = this->loop_->TcpServer_->addNewConnection(targer_fd);
                    conn->setNeedReply(false);

                } else {
                    this->loop_->doHandshake();
                }
            }
        }
    }
}

HttpConnectRequest::~HttpConnectRequest()
{ ::close(this->fd_); }

void HttpConnectRequest::setType(IoType type)
{ this->head_.type_ = type; }

void HttpConnectRequest::setFd(int fd)
{ this->fd_ = fd; }

void HttpConnectRequest::setAddr(const char* ip, int port) {
    this->addr_.sin_family = AF_INET;
    this->addr_.sin_port = htons(port);
    this->addr_.sin_addr.s_addr = ::inet_addr(ip);
}

void HttpConnectRequest::setLoop(EventLoop* loop)
{ this->loop_ = loop; }

void HttpConnectRequest::onComplete(int res_bytes) {

    if(this->loop_ && this->head_.type_ == IoType::HTTP_CONNECT) {
        if(res_bytes < 0) {
            int err = -res_bytes;
            std::cerr << "[Handshake Error] Failed in state " 
                    << ", error: " << strerror(err) << std::endl;

            if(++this->error_count_ >= 5) {
                std::cerr << "[Handshake Fatal] Max retries reached. Giving up." << std::endl;
                this->reset();

                return;
            }

            if(this->fd_ >= 0) {
                ::close(this->fd_);
                this->fd_ = -1;
            }

            this->loop_->addTimer(5000, [this] () {
                this->loop_->startConnectHttp();
            }, this->loop_->HttpConnect_timer_req_.get());

        } else {
            int targer_fd = this->fd_;
            this->fd_ = 0;

            TcpConnectionPtr conn = this->loop_->TcpServer_->addNewConnection(targer_fd);
            this->loop_->HttpConnectCallback_(conn);
        }
    }

}

void HttpConnectRequest::reset() {
    if(this->fd_ > 0) {
        ::close(this->fd_);
        this->fd_ = -1;
    }

    this->error_count_ = 0;
}

void SendReplicaRequest::setType(IoType type)
{ this->head_.type_ = IoType::REPL_SEND; }

void SendReplicaRequest::setConn(const TcpConnectionPtr& conn)
{ this->conn_ = conn; }

void SendReplicaRequest::setLoop(EventLoop* loop)
{ this->loop_ = loop; }

void SendReplicaRequest::onComplete(int res_bytes) {

    if(this->conn_ && this->loop_ && this->head_.type_ == IoType::REPL_SEND) {
        if(res_bytes > 0) {
            this->conn_->addReplOffset(res_bytes);
            // std::cout << conn_->fd() << std::endl;
            // std::cout << conn_->getReplicaOffset() << std::endl;
            // std::cout << this->loop_->repl_backlog_.getMasterOffset() << std::endl;

            if(this->conn_->getReplicaOffset() < this->loop_->repl_backlog_.getMasterOffset()) {
                this->loop_->sendToReplica(this->conn_, this);
            }
        }
    }

}

};

};