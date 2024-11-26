#ifndef MEMDB_CORE_COLUMN_H
#define MEMDB_CORE_COLUMN_H

#include "memdb/core/DataType.h"
#include "memdb/core/Value.h"

#include "memdb/core/enums/ColumnAttribute.h"

#include <string>
#include <optional>
#include <vector>
#include <stdexcept>

namespace memdb {
namespace core {

class Column {
public:
    Column(const std::string& name, const DataType& type,
           const std::vector<ColumnAttribute>& attributes = {},
           const std::optional<Value>& default_value = std::nullopt);

    const std::string& get_name() const;
    const DataType& get_type() const;
    const std::vector<ColumnAttribute>& get_attributes() const;
    const std::optional<Value>& get_default_value() const;

    bool has_attribute(ColumnAttribute attribute) const;

    std::string to_string() const;

private:
    std::string name_;
    DataType type_;
    std::vector<ColumnAttribute> attributes_;
    std::optional<Value> default_value_;
};

} // namespace core
} // namespace memdb

#endif // MEMDB_CORE_COLUMN_H