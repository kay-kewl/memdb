#include "memdb/core/ResultRow.h"

namespace memdb {
namespace core {

ResultRow::ResultRow(const std::vector<std::optional<Value>>& values, const std::vector<ColumnInfo>& columns)
    : row_map_() 
{
    for (size_t i = 0; i < columns.size(); ++i) {
        if (values[i].has_value()) {
            row_map_[columns[i].name] = values[i].value();
        }
    }
}

const std::optional<Value>& ResultRow::operator[](const std::string& column_name) const {
    auto it = row_map_.find(column_name);
    if (it != row_map_.end()) {
        return it->second;
    }
    static std::optional<Value> empty;
    return empty;
}

}
} 