#ifndef MEMDB_CORE_STRUCTS_TOKEN_H
#define MEMDB_CORE_STRUCTS_TOKEN_H

#include "memdb/core/enums/TokenType.h"

#include <string>

namespace memdb {
namespace core {

struct Token {
    TokenType type;
    std::string value;
};

} 
} 

#endif // MEMDB_CORE_STRUCTS_TOKEN_H