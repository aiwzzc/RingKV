#include "RouteBatchTask.h"
#include "CommandHandlers.h"

#include "base/jemalloc.h"

using namespace AeroIO::net;

namespace rkv {

void RouteBatchTask::reset() {
    for(int i = 0; i < this->cmd_count_; ++i) {
        this->cmds[i].buffer_ = std::string("");
        this->cmds[i].tokens_.clear();
    }

    this->cmd_count_ = 0;
    this->conn_.reset();
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

    this->current_loop_->runInLoop([this] () mutable {
        if(this->conn_) {
            for(int i = 0; i < this->cmd_count_; ++i) {
                if(this->cmds[i].slot_id_ != 0) {
                    this->conn_->fillSingleSlot(this->cmds[i].slot_id_, std::move(this->cmds[i].response_data_));
                }
            }

            this->conn_->tryFlushResponses();
        }
    });
}

RouteBatchTaskPool::RouteBatchTaskPool() { INIT_LIST_HEAD(&this->head_); }
RouteBatchTaskPool::~RouteBatchTaskPool() {
    while(this->head_.next_ != &this->head_) {
        RouteBatchTask* task = container_of(this->head_.next_, RouteBatchTask, node_);
        list_del(this->head_.next_);

        this->mempool_->free(task);
    }
}

void RouteBatchTaskPool::initPool() {
    for(int i = 0; i < TASKPOOLSIZE; ++i) {
        void*p = this->mempool_->alloc(sizeof(RouteBatchTask));
        if(p == nullptr) return;

        RouteBatchTask* task = new (p) RouteBatchTask();
        list_add(&this->head_, &task->node_);
    }
}

RouteBatchTaskPool& RouteBatchTaskPool::getInstance() {
    static thread_local RouteBatchTaskPool instance;
    return instance;
}

RouteBatchTaskPtr RouteBatchTaskPool::get(EventLoop* current_loop) {
    RouteBatchTask* task = nullptr;
    if(this->head_.next_ == &this->head_) {
        void*p = this->mempool_->alloc(sizeof(RouteBatchTask));
        if(p == nullptr) return nullptr;

        task = new (p) RouteBatchTask();

        return std::shared_ptr<RouteBatchTask>(task, [current_loop] (RouteBatchTask* Task) {
            current_loop->runInLoop([Task] () {
                RouteBatchTaskPool::getInstance().release(Task);
            });
        });
    }

    task = container_of(this->head_.next_, RouteBatchTask, node_);
    list_del(this->head_.next_);

    return std::shared_ptr<RouteBatchTask>(task, [current_loop] (RouteBatchTask* Task) {
        current_loop->runInLoop([Task] () {
            RouteBatchTaskPool::getInstance().release(Task);
        });
    });
}

void RouteBatchTaskPool::release(RouteBatchTask* task) {
    task->reset();
    list_add(&this->head_, &task->node_);
}

void RouteBatchTaskPool::setMempool(rkv::JemallocWrapper* mempool)
{ this->mempool_ = mempool; }

};