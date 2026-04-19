#pragma once

#include <functional>
#include <liburing.h>
#include <deque>
#include <memory>
#include <atomic>
#include <unordered_set>

#ifdef BLOCK_SIZE
#undef BLOCK_SIZE
#endif

#include "Buffer.h"
#include "IoRequest.h"
#include "../../concurrentqueue/concurrentqueue.h"

namespace rkv {

class Ringengine;
struct Config;

};

namespace AeroIO {

namespace net {

constexpr uint64_t WAKEUP_MSG_DATA = 0xFFFFFFFFFFFFFFFF; // 目标线程收到的空投标识
constexpr uint64_t SENDER_IGNORE_MSG = 0xFFFFFFFFFFFFFFFE; // 发送线程自己收到的完成标识
constexpr std::size_t kEntresLength = 8192;
constexpr std::size_t FIXEDFDSIZE = 2000;
constexpr std::size_t MAX_FIXED_FDS = 10000;

constexpr std::size_t AOFREWRITETHRESHOLD   = 64 * 1024 * 1024;
// constexpr std::size_t AOFREWRITETHRESHOLD = 1024;
constexpr const char* AOFBASEFILEPATH       = "../AeroIO/data/aof/appendonly-";
constexpr const char* AOFSUFFIXFILEPATH     = ".aof";
constexpr const char* AOFTEMPSUFFIXFILEPATH = ".temp.aof";
constexpr const char* RDBBASEFILEPATH       = "../AeroIO/data/rdb/dump-";
constexpr const char* RDBSUFFIXFILEPATH     = ".rdb";

constexpr const char* HANDSHAKEORDER        = "SYNC_HANDSHAKE-";
constexpr const char* HANDSHAKESUFFIX       = "\r\n";

constexpr int ACTIVEEXPIRESIZE              = 20;

class TcpServer;

class EventLoop {

public:
    friend struct AofRequest;
    friend struct AoffsyncRequest;
    friend struct TimerRequest;
    friend struct AofRewriteRequest;
    friend struct RdbWriteRequest;
    friend struct HandShakeRequest;
    friend struct SendReplicaRequest;
    friend struct HttpConnectRequest;

    using Functor = std::function<void()>;
    using TimerCallback = std::function<void()>;
    using HttpConnectCallback = std::function<void(const TcpConnectionPtr&)>;

    EventLoop(rkv::Ringengine* engine);
    ~EventLoop();

    static EventLoop* getEventLoopOfCurrentThread();

    bool isInLoopThread() const;
    void runInLoop(Functor cb);
    void setTcpServer(TcpServer* server);
    TcpServer* getTcpServer();
    void loop();
    void quit();
    io_uring* ring();
    void addTimer(uint64_t ms, const TimerCallback& cb, TimerRequest* timer);

    void appendAof(char*, std::size_t);
    void setPersistFileIndex(int index);
    const char* getAofFilePath();
    const char* getRdbFilePath();
    std::size_t& getAofFileOffset();

    void queueInfreeFixedFds(int index);

    int getFreeFixedFd();
    int getNextIndex();
    std::vector<int>& getFixedFds();
    int getConnIndex();
    rkv::Ringengine* getEngine();

    bool rdbIsSaving();
    UringBuffer* rdbWriteBuffer();
    std::size_t rdbCowBuchetIndex();
    std::unordered_set<std::string>& rdbCowSavedKeys();

    void startHandshake();
    void doHandshake();
    void startFullSync(const TcpConnectionPtr& conn);
    void addToReplicas_(const TcpConnectionPtr& conn);
    void removeFromReplicas_(const TcpConnectionPtr& conn);
    ReplBacklog* replBacklog();
    void appendReplicationBacklog(const std::string& cmd_data);

    void setLoopsEngines(std::vector<std::pair<EventLoop*, rkv::Ringengine*>>*);
    std::vector<std::pair<EventLoop*, rkv::Ringengine*>>* getLoopsEngines();

    void startConnectHttp();
    void setHttpConnectCallback(const HttpConnectCallback& cb);

private:
    int createEventfd();
    void wakeup();
    void sendMsgRing(EventLoop* senderLoop, int target_ring_fd);
    bool doPendingFunctors();
    void armWakeupFd();
    void initReq();

    void tryFlushAof();
    void tryFsyncAof();
    void startAofRewrite();
    void doAofRewriteStep();
    void tryFlushTempAof();
    void finishAofRewrite();
    void initAof();

    void startRdb();
    void doRdbWriteStep();
    void initRdb();
    void tryFlushRdb();

    void sendToReplica(const TcpConnectionPtr& conn, SendReplicaRequest* req);
    void start_Active_Expire();
    void do_Active_Expire();

    io_uring* ring_;
    rkv::Ringengine* engine_;
    bool quit_;
    std::thread::id threadId_;
    int wakeupfd_;
    uint64_t wakeup_buf_;
    TcpServer* TcpServer_;

    std::vector<int> fixed_fds_;
    std::deque<int> free_fixed_fds_;
    int fixed_fds_index_;

    // MPSC
    std::unique_ptr<moodycamel::ConcurrentQueue<Functor>> pending_functors_;
    std::vector<std::pair<EventLoop*, rkv::Ringengine*>>* LoopsEngines_;

    UringBuffer aof_write_buffer_;
    UringBuffer aof_flush_buffer_;
    UringBuffer aof_rewrite_flush_buffer_;
    UringBuffer aof_rewrite_buffer_;
    int aof_fd_;
    int temp_aof_fd_;
    bool aof_is_flushing_;
    bool aof_fsync_in_flight_;
    bool aof_is_rewriting_;
    bool Replica_if_Syncing_;
    std::size_t aof_file_offset_;
    std::size_t temp_aof_file_offset_;
    std::string aof_file_path_;
    std::string temp_aof_path_;
    int aof_fsync_error_count_;
    std::size_t aof_rewrite_bucket_cursor_;

    int rdb_fd_;
    bool rdb_is_saving_;
    std::string rdb_file_path_;
    UringBuffer rdb_write_buffer_;
    std::unordered_set<std::string> rdb_cow_saved_keys_;
    std::size_t rdb_cow_bucket_cursor_;

    std::unique_ptr<AofRequest> aof_req_;
    std::unique_ptr<AoffsyncRequest> aoffsync_req_;
    std::unique_ptr<TimerRequest> aof_timer_req_;
    std::unique_ptr<TimerRequest> rdb_timer_req_;
    std::unique_ptr<TimerRequest> handShake_timer_req_;
    std::unique_ptr<TimerRequest> actExpire_timer_req_;
    std::unique_ptr<TimerRequest> HttpConnect_timer_req_;
    std::unique_ptr<AofRewriteRequest> aofrewrite_req_;
    std::unique_ptr<RdbWriteRequest> rdbWrite_req_;
    std::unique_ptr<HandShakeRequest> handShake_req_;
    std::unique_ptr<SendReplicaRequest> sendRepl_req_;
    std::unique_ptr<HttpConnectRequest> HttpConnect_req_;
    int connect_index_;
    std::string handShake_instruction_;

    std::vector<std::pair<TcpConnectionPtr, std::unique_ptr<SendReplicaRequest>>> replicas_;
    ReplBacklog repl_backlog_; // RingBuffer: no expansion std::vector<char>
    bool repl_is_writing_;

    HttpConnectCallback HttpConnectCallback_;
    std::atomic<bool> is_sleeping_{false};

};

};

};