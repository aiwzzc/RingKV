#pragma once

namespace rkv {

class JemallocWrapper;
class Ringengine;

struct ServerContext {
    rkv::JemallocWrapper* mempool;
    rkv::Ringengine* engine;
};

};