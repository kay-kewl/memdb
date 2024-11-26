#ifndef MEMDB_CORE_ENUMS_COLUMNATTRIBUTE_H
#define MEMDB_CORE_ENUMS_COLUMNATTRIBUTE_H

namespace memdb {
namespace core {

enum class ColumnAttribute {
    Unique,
    AutoIncrement,
    Key
};

} // namespace core
} // namespace memdb

#endif // MEMDB_CORE_ENUMS_COLUMNATTRIBUTE_H