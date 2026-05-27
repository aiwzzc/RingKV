#include "ringClient.h"

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

void ringClient::fillSingleSlot(uint64_t slot_id, std::string&& data) {
    if(!this->conn_->connected() || slot_id == 0) return;

    uint64_t front_id = this->pending_responses_.front().id_;

    if(slot_id >= front_id && (slot_id - front_id) < this->pending_responses_.size()) {
        auto& slot = this->pending_responses_[slot_id - front_id];
        slot.data_ = std::move(data);
        slot.is_ready_ = true;
    }
}

uint64_t ringClient::appendPendRes(ResponseSlot& slot) {
    slot.id_ = this->next_slot_id_++;
    this->pending_responses_.push_back(std::move(slot));

    return slot.id_;
}

void ringClient::tryFlushResponses() {
    while(!this->pending_responses_.empty()) {
        auto& slot = this->pending_responses_.front();
        if(!slot.is_ready_) break;

        if(!this->conn_->tryFillReplyBuffer(slot.data_)) break;
        this->pending_responses_.pop_front();
    }

    this->conn_->flushWriteBatch();
}

};