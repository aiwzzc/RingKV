#pragma once

namespace rkv {

struct RedisObject;
class JemallocWrapper;

class robject {

public:
    static void freeRedisObject(RedisObject* obj, JemallocWrapper* mempool);

private:


};

};