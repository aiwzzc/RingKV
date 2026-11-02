#pragma once

#include <any>
#include <deque>
#include <memory>
#include <string>

#include "Buffer.h"
#include "Callbacks.h"
#include "Socket.h"
#include "IoRequest.h"
#include "PendingWrite.h"
#include "common.h"

namespace AeroIO::net {
class EventLoop;

class TcpConnection : public std::enable_shared_from_this<TcpConnection> {
public:
    TcpConnection(EventLoop* loop, int socket, PoolContext& ctx);
    ~TcpConnection();
    enum class StateE { kDisconnected, kConnecting, kConnected, kDisconnecting };

    UringBuffer* getInputBuffer();
    EventLoop* getLoop() const;
    int fd() const;
    bool connected() const;
    bool disconnected() const;
    void send(std::string data);
    void flushWriteBatch();
    ReplyBufferPtr getCurrentReplyBuffer();
    bool tryFillReplyBuffer(std::string& data);

    void setMessageCallback(const MessageCallback&);
    void setCloseCallback(const CloseCallback&);
    void setConnectionCallback(const ConnectionCallback&);
    void setWriteCompleteCallback(const WriteCompleteCallback&);
    void setTcpNoDelay(bool on);
    void setReusePort(bool on);
    void setContext(const std::any& context);
    const std::any& getContext() const;
    std::any* getMutableContext();

    void setFixedFileIndex(int index);
    int getFixedIndex() const;
    void setNoregister();
    void start();
    void send_start();
    void send_schedul(int result);
    void handleRead(int result);
    void handleClose();
    void onPostClosereq();
    void handleError();
    void forceClose();
    // Only after the loop has synchronously cancelled all I/O.
    void stopAfterIoCancelled();
    void connectEstablished();
    void connectDestroyed();

private:
    EventLoop* loop_;
    StateE state_;
    std::unique_ptr<Socket> socket_;
    PoolContext Poolctx_;
    std::unique_ptr<IoRequest> read_req_;
    ReplyBufferPtr current_reply_;
    ReplyBufferPtr writing_reply_;
    std::deque<ReplyBufferPtr> backlog_;
    bool is_writing_ = false;
    bool read_in_flight_ = false;
    bool write_in_flight_ = false;
    MessageCallback messageCallback_;
    CloseCallback closeCallback_;
    ConnectionCallback connectionCallback_;
    WriteCompleteCallback writeCompleteCallback_;
    UringBuffer inputBuffer_;
    std::deque<BlockPtr> recv_blocks_;
    BlockPtr active_recv_block_;
    std::any context_;
};
using TcpConnectionPtr = std::shared_ptr<TcpConnection>;
}
