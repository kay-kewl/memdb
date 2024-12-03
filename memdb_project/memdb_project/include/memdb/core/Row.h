#ifndef MEMDB_CORE_ROW_H
#define MEMDB_CORE_ROW_H

#include "memdb/core/Value.h"

#include <vector>
#include <optional>
#include <cstdint>
#include <stdexcept>

namespace memdb {
namespace core {

using RowID = uint32_t;

class Row {
public:
    Row(RowID id, const std::vector<std::optional<Value>>& values);

    RowID get_id() const;

    std::vector<std::optional<Value>>& get_values();
    const std::vector<std::optional<Value>>& get_values() const;

    std::optional<Value>& get_value(size_t index);
    const std::optional<Value>& get_value(size_t index) const;

    void set_value(size_t index, const Value& value);

private:
    RowID id_;
    std::vector<std::optional<Value>> values_;
};

} 
} 

#endif // MEMDB_CORE_ROW_H