#pragma once

// Linux headers and concurrentqueue both use BLOCK_SIZE.
#pragma push_macro("BLOCK_SIZE")
#undef BLOCK_SIZE
#include "concurrentqueue/concurrentqueue.h"
#pragma pop_macro("BLOCK_SIZE")
