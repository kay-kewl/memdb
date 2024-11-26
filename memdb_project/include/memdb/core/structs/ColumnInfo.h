#ifndef MEMDB_CORE_STRUCTS_COLUMNINFO_H
#define MEMDB_CORE_STRUCTS_COLUMNINFO_H

#include "memdb/core/enums/Type.h"
#include "memdb/core/DataType.h"

#include <string>

namespace memdb {
namespace core {

struct ColumnInfo {
    std::string name;
    DataType type;

    ColumnInfo(const std::string& name_, Type type_) : name(name_), type(type_) {}
    ColumnInfo(const std::string& name_, const DataType& data_type) : name(name_), type(data_type) {} 

    const std::string& get_name() const { return name; }

    Type get_type() const { return type.get_type(); }
};

} // namespace core
} // namespace memdb

#endif // MEMDB_CORE_STRUCTS_COLUMNINFO_H