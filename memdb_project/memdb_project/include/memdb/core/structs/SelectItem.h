#ifndef MEMDB_CORE_STRUCTS_SELECTITEM_H
#define MEMDB_CORE_STRUCTS_SELECTITEM_H

#include <memdb/core/Expression.h>
#include <string>

namespace memdb {
namespace core {

struct SelectItem {
    std::unique_ptr<Expression> expression;
    std::string alias; 
};

}
}

#endif // MEMDB_CORE_STRUCTS_SELECTITEMS_H