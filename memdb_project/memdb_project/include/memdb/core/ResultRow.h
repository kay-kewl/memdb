#ifndef MEMDB_CORE_RESULTROW_H
#define MEMDB_CORE_RESULTROW_H

#include "memdb/core/Value.h"
#include "memdb/core/structs/ColumnInfo.h"

#include <vector>
#include <optional>
#include <unordered_map>
#include <string>

namespace memdb {
namespace core {

class ResultRow {
public:
    ResultRow(const std::vector<std::optional<Value>>& values, const std::vector<ColumnInfo>& columns);

    const std::optional<Value>& operator[](const std::string& column_name) const;

private:
    std::unordered_map<std::string, std::optional<Value>> row_map_;
};

} 
} 

#endif // MEMDB_CORE_RESULTROW_H