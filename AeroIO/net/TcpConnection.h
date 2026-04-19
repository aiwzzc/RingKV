#pragma once

#include <memory>
#include <any>
#include <string>
#include <deque>

#include "Buffer.h"
#include "Callbacks.h"
#include "Socket.h"
#include "IoRequest.h"
#include "PendingWrite.h"
#include "common.h"

namespace rkv {

class JemallocWrapper;
class Ringengine;
struct ServerContext;
struct CommandDef;

};

namespace AeroIO {

namespace net {

constexpr std::size_t BUFFERSIZE            = 4096;
constexpr std::size_t PENDINGWRITEPOOLSIZE  = 4096;

class EventLoop;

struct ResponseSlot {
    uint64_t id_{0};
    bool is_ready_{false};
    std::string data_;
};

enum class ConnState {
    HANDSHAKING,  // 刚连上的初始状态
    NORMAL_CLIENT,// 普通客户端
    REPLICA       // 从节点
};

class TcpConnection : public std::enable_shared_from_this<TcpConnection> {

public:
    TcpConnection(EventLoop* loop, int socket, rkv::ServerContext* ctx);
    ~TcpConnection();

    enum class StateE { kDisconnected, kConnecting, kConnected, kDisconnecting };

    UringBuffer* getInputBuffer();
    EventLoop* getLoop() const;
    int fd() const;
    ConnState getConnState();

#if 0
    void send(const void* message, int len);
    void send(std::string_view message);
    void send(UringBuffer* message);
    void send_schedul(PendingWrite* pw);
#endif

    void flushWriteBatch();
    ReplyBufferPtr getCurrentReplyBuffer();
    void send_start();
    void send_schedul(int res_bytes);
    uint64_t appendPendRes(ResponseSlot&);
    void fillSingleSlot(uint64_t slot_id, std::string&& data);
    void fillPendingSlots(std::vector<std::pair<std::string, uint64_t>>&&);
    // ResponseSlot* getLastPendingSlot();
    void tryFlushResponses();

    bool connected() const;
    bool disconnected() const;

    void setMessageCallback(const MessageCallback&);
    void setCloseCallback(const CloseCallback&);
    void setConnectionCallback(const ConnectionCallback&);
    void setWriteCompleteCallback(const WriteCompleteCallback&);

    void setTcpNoDelay(bool on);
    void setReusePort(bool on);
    void setFixedFileIndex(int index);
    void setNoregister();

    void setConnState(ConnState state);
    void setContext(const std::any& context);
    const std::any& getContext() const;
    std::any* getMutableContext();
    int getFixedIndex() const;
    std::vector<RouteBatch>& route_batches();
    int pendResIndex();

    void handleRead(int res_bytes);
    void handleWrite();
    void handleClose();
    void onPostClosereq();
    void handleError();

    void forceClose();
    void shutdown();
    void detachFromLoop();
    void attachToLoop(EventLoop* newLoop);
    void setIsReplica(bool yes);
    bool IsReplica();
    void setNeedReply(bool yes);
    bool NeedReply();

    void connectEstablished();
    void connectDestroyed();
    void start();

    std::size_t getReplicaOffset() const;
    void addReplOffset(std::size_t offset);

    void send(const char*, std::size_t);
    void appendToPendingWrite(const char*, std::size_t);
    void send();

    std::string fragmented_buffer_;

private:

    void flush_backlog();
#if 0
    std::unique_ptr<PendingWritePool> pwPool_;
#endif

    EventLoop* loop_;
    StateE state_;
    ConnState connState_;
    std::unique_ptr<Socket> socket_;
    rkv::ServerContext* Serverctx_;
    std::unique_ptr<IoRequest> read_req_;
    std::unique_ptr<CloseRequest> close_req_;
    
    ReplyBufferPtr current_reply_;
    ReplyBufferPtr writing_reply_;
    std::unique_ptr<PendingWrite> send_pending_;
    std::deque<ReplyBufferPtr> backlog_;
    std::deque<ResponseSlot> pending_responses_;
    bool is_replica_{false};
    bool need_reply_{true};
    bool is_writing_{false};
    std::size_t replica_offset_{0};
    uint64_t next_slot_id_{1};

    std::vector<RouteBatch> route_batches_;

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

};

};