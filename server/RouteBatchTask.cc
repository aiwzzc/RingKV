#include "RouteBatchTask.h"
#include "CommandHandlers.h"

#include "base/jemalloc.h"

using namespace AeroIO::net;

namespace rkv {

void RouteBatchTask::reset() {
    for (int i = 0; i < cmd_count_; ++i) {
        cmds[i].tokens_.clear();
        cmds[i].buffer_ = std::string{};
        cmds[i].response_data_.clear();
    }

    this->cmd_count_ = 0;
    this->clinet_.reset();
}

void RouteBatchTask::operator()() {

    std::unordered_map<std::string_view, std::string> local_notwrite_cache;
    std::unordered_map<std::string_view, std::pair<std::vector<std::string_view>, std::vector<int>>> local_last_write_slot;

    for(int i = 0; i < this->cmd_count_; ++i) {
        if(local_notwrite_cache.contains(this->cmds[i].tokens_[1]) && !this->cmds[i].cmd_def_->is_write_) {
            std::string hot_data = local_notwrite_cache[this->cmds[i].tokens_[1]];
            if(cmds[i].slot_id_ != 0) this->cmds[i].response_data_ = std::move(hot_data);
            continue;

        } else if(this->cmds[i].cmd_def_->is_write_) {
            local_last_write_slot[this->cmds[i].tokens_[1]].first = this->cmds[i].tokens_;
            local_last_write_slot[this->cmds[i].tokens_[1]].second.push_back(i);
            continue;
        }

        rkv::CommandContext ctx{cmds[i].tokens_, this->target_engine_, true, this->current_loop_, this->target_loop_, "", false};

        this->execute_(ctx, cmds[i].cmd_def_);
        if(cmds[i].slot_id_ != 0) {
            if(!this->cmds[i].cmd_def_->is_write_) local_notwrite_cache[this->cmds[i].tokens_[1]] = ctx.response_;
            cmds[i].response_data_ = std::move(ctx.response_);
        }
    }

    for(auto it = local_last_write_slot.begin(); it != local_last_write_slot.end(); ++it) {
        rkv::CommandContext ctx{it->second.first, this->target_engine_, true, this->current_loop_, this->target_loop_, "", false};
        this->execute_(ctx, this->cmds[it->second.second.back()].cmd_def_);

        for(auto& task_index : it->second.second) {
            std::string delay_res = ctx.response_;

            if(this->cmds[task_index].slot_id_ != 0) {
                this->cmds[task_index].response_data_ = std::move(delay_res);
            }
        }
    }

    auto self = shared_from_this();

    this->current_loop_->runInLoop([self] () mutable {
        if(self->clinet_) {
            for(int i = 0; i < self->cmd_count_; ++i) {
                if(self->cmds[i].slot_id_ != 0) {
                    self->clinet_->fillSingleSlot(self->cmds[i].slot_id_, std::move(self->cmds[i].response_data_), 5);
                }
            }

            self->clinet_->tryFlushResponses();
        }
    });
}

RouteBatchTaskPool::RouteBatchTaskPool(rkv::JemallocWrapper& mempool) : mempool_(mempool) { free_.reserve(TASKPOOLSIZE); }

RouteBatchTaskPool::~RouteBatchTaskPool() {
    for (auto* task : free_) {
        task->~RouteBatchTask();
        mempool_.free(task);
    }
}

RouteBatchTaskPtr RouteBatchTaskPool::get(EventLoop* owner) {
    RouteBatchTask* task;
    if (free_.empty()) {
        void* storage = mempool_.alloc(sizeof(RouteBatchTask));
        if (!storage) throw std::bad_alloc();
        try { task = new (storage) RouteBatchTask(); }
        catch (...) { mempool_.free(storage); throw; }
    } else {
        task = free_.back();
        free_.pop_back();
    }
    return RouteBatchTaskPtr(task, [this, owner](RouteBatchTask* returned) {
        owner->runInLoop([this, returned] { release(returned); });
    });
}

void RouteBatchTaskPool::release(RouteBatchTask* task) {
    task->reset();
    // Pool capacity is an optimization, not an allocation requirement at shutdown.
    if (free_.size() < free_.capacity()) free_.push_back(task);
    else {
        task->~RouteBatchTask();
        mempool_.free(task);
    }
}
}
