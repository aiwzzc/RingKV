#pragma once

namespace rkv {

constexpr const char* SETERRSTR = "-ERR wrong number of arguments for 'set' command\r\n";
constexpr const char* GETERRSTR = "-ERR wrong number of arguments for 'get' command\r\n";
constexpr const char* SETNXERRSTR = "-ERR wrong number of arguments for 'setnx' command\r\n";
constexpr const char* DELERRSTR = "-ERR wrong number of arguments for 'del' command\r\n";
constexpr const char* EXISTERRSTR = "-ERR wrong number of arguments for 'exist' command\r\n";
constexpr const char* INCRERRSTR = "-ERR wrong number of arguments for 'incr' command\r\n";
constexpr const char* INCRBYERRSTR = "-ERR wrong number of arguments for 'incrby' command\r\n";
constexpr const char* DECRERRSTR = "-ERR wrong number of arguments for 'decr' command\r\n";
constexpr const char* DECRBYERRSTR = "-ERR wrong number of arguments for 'decrby' command\r\n";
constexpr const char* LPUSHERRSTR = "-ERR wrong number of arguments for 'lpush' command\r\n";
constexpr const char* RPUSHERRSTR = "-ERR wrong number of arguments for 'rpush' command\r\n";
constexpr const char* LPOPERRSTR = "-ERR wrong number of arguments for 'lpop' command\r\n";
constexpr const char* RPOPERRSTR = "-ERR wrong number of arguments for 'rpop' command\r\n";
constexpr const char* LRANGEERRSTR = "-ERR wrong number of arguments for 'lrange' command\r\n";
constexpr const char* LLENERRSTR = "-ERR wrong number of arguments for 'llen' command\r\n";
constexpr const char* LINDEXERRSTR = "-ERR wrong number of arguments for 'lindex' command\r\n";
constexpr const char* LSETERRSTR = "-ERR wrong number of arguments for 'lset' command\r\n";
constexpr const char* FLUSHALLERRSTR = "-ERR wrong number of arguments for 'flushall' command\r\n";
constexpr const char* USEWRONGTYPEERRSTR = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
constexpr const char* OVERFLOWERRSTR = "-ERR increment or decrement would overflow\r\n";
constexpr const char* NOTINTOROUTRANGEERRSTR = "-ERR value is not an integer or out of range\r\n";
constexpr const char* POSITIVEERRORSTR = "ERR value is out of range, must be positive";
constexpr const char* INTERNALERRSTR = "-ERR internal error\r\n";
constexpr const char* SYNTAXERRSTR = "-ERR syntax error\r\n";
constexpr const char* STRINGERRSTR = "$-1\r\n";
constexpr const char* OTHERERRSTR = "-ERR other error\r\n";
constexpr const char* NOSUCHKEYERRSTR = "-ERR no such key\r\n";
constexpr const char* INDEXOUTRANGEERRSTR = "-ERR index out of range\r\n";
constexpr const char* PROTOCOLERRORSTR = "-ERR Protocol error\r\n";
constexpr const char* UNKNOWNCOMMANDSTR = "-ERR unknown command\r\n";
constexpr const char* WRONGNUMBERSTR = "-ERR wrong number of arguments\r\n";
constexpr const char* VALUENOTDOUBLE = "-ERR value is not a valid double\r\n";
constexpr const char* UNKNOWNERRORSTR = "-ERR unknown error\r\n";

constexpr const char* PONGSTR = "+PONG\r\n";
constexpr const char* OKSTR = "+OK\r\n";
constexpr const char* ZEROSTR = ":0\r\n";
constexpr const char* ONESTR = ":1\r\n";
constexpr const char* EMPTYARRAYSTR = "*0\r\n";

};