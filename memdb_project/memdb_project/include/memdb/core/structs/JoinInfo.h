#ifndef MEMDB_CORE_STRUCTS_JOININFO_H
#define MEMDB_CORE_STRUCTS_JOININFO_H

#include "memdb/core/Expression.h"

#include <string>

namespace memdb {
namespace core {

struct JoinInfo {
    std::string table_name;
    std::string table_alias;
    std::unique_ptr<Expression> join_condition;
};

}
}

#endif // MEMDB_CORE_STRUCTS_JOININFO_H