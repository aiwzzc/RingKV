#pragma once

namespace AeroIO {
namespace net {

class ReplyBufferPool;
class BlockPool;

struct PoolContext {
    ReplyBufferPool* replyBufferPool;
    BlockPool* blockPool;
};

};

};