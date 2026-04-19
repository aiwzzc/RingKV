#include "robject.h"
#include "common.h"
#include "jemalloc.h"
#include "list.h"
#include "kvstr.h"
#include "zset.h"

namespace rkv {

void robject::freeRedisObject(RedisObject* obj, JemallocWrapper* mempool) {
    if(obj == nullptr) return;

    switch(obj->type) {
        case RedisType::OBJ_STRING: {
            if(obj->ptr && obj->encoding == OBJ_ENCODING_RAW) {
                kvstr* s = (kvstr*)obj->ptr;
            
                mempool->free(s);
            }
            mempool->free(obj);

            break;
        }

        case RedisType::OBJ_LIST: {
            if(obj->ptr) {
                ListObject* list = (ListObject*)obj->ptr;
                list->release_all_node(mempool);

                mempool->free(obj->ptr);
            }
            mempool->free(obj);

            break;
        }

        case RedisType::OBJ_ZSET: {
            if(obj->ptr) {
                ZSetObject* zset = (ZSetObject*)obj->ptr;
                zset->release_all_node(mempool);

                mempool->free(obj->ptr);
            }
            mempool->free(obj);

            break;
        }
    }

}

};

