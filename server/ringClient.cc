#include "ringClient.h"
#include "net/EventLoop.h"
#include "Log/LogApi.h"

namespace rkv {

ringClient::ringClient(
    const AeroIO::net::TcpConnectionPtr& conn, 
    std::size_t loops): 
    conn_(conn), route_cmds_per_loop_(loops) {

    this->need_reply_ = this->conn_->NeedReply();
}

ringClient::~ringClient() {
    this->release();
}

void ringClient::release() {
    this->conn_.reset();
    this->route_cmds_per_loop_.clear();
}

RouteCommandPerLoop& ringClient::route_cmds_per_loop() {
    return this->route_cmds_per_loop_;
}

void ringClient::fillSingleSlot(uint64_t slot_id, std::string&& data, int num) {
    assert(conn_->getLoop()->isInLoopThread());

    if(slot_id == 0) LOG_FATAL("fillSingleSlot Slot_id == 0");
    
    uint64_t front_id = this->pending_responses_.front().id_;

    if(slot_id >= front_id && (slot_id - front_id) < this->pending_responses_.size()) {
        auto& slot = this->pending_responses_[slot_id - front_id];
        slot.data_ = std::move(data);
        slot.is_ready_ = true;

    } else {
        LOG_FATAL("number:{} slot_id:{} front_id:{} size:{} next_slot_id:{} client_ptr:{} conn_fd:{}",
        num, slot_id, front_id, pending_responses_.size(),
        this->next_slot_id_, (void*)this, conn_->fd());
    }
}

uint64_t ringClient::appendPendRes(ResponseSlot& slot) {
    slot.id_ = this->next_slot_id_++;
    assert(slot.is_ready_ == false); 
    this->pending_responses_.push_back(std::move(slot));

    return slot.id_;
}

int ringClient::slot_size() 
{ return this->pending_responses_.size(); }

bool ringClient::slot_is_ready(int slot_id) {
    bool is_ready{false};
    uint64_t front_id = this->pending_responses_.front().id_;

    if(slot_id >= front_id && (slot_id - front_id) < this->pending_responses_.size()) {
        auto& slot = this->pending_responses_[slot_id - front_id];
        is_ready = slot.is_ready_;
    }

    return is_ready;
}

void ringClient::tryFlushResponses() {
    while(!this->pending_responses_.empty()) {
        auto& slot = this->pending_responses_.front();
        if(!slot.is_ready_) break;

        if(!this->conn_->tryFillReplyBuffer(slot.data_)) {
            // LOG_INFO("tryFillReplyBuffer false");
            break;
        }
        this->pending_responses_.pop_front();
    }

    this->conn_->flushWriteBatch();
}

};