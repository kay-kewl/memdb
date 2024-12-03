#ifndef MEMDB_CORE_INDEX_H
#define MEMDB_CORE_INDEX_H

#include "memdb/core/Value.h"
#include "memdb/core/Row.h"
#include "memdb/core/enums/IndexType.h"

#include <vector>
#include <unordered_map>
#include <map>
#include <string>
#include <memory>

namespace memdb {
namespace core {

class Index {
public:
    Index(IndexType type, const std::vector<std::string>& columns);

    IndexType get_type() const { return type_; }
    const std::vector<std::string>& get_columns() const { return columns_; }

    void add_row(RowID row_id, const std::unordered_map<std::string, Value>& row);
    void remove_row(RowID row_id, const std::unordered_map<std::string, Value>& row);

    std::vector<RowID> search_unordered(const std::unordered_map<std::string, Value>& condition) const;
    std::vector<RowID> search_ordered(const std::string& column,
                                      const std::optional<Value>& lower,
                                      bool lower_inclusive,
                                      const std::optional<Value>& upper,
                                      bool upper_inclusive) const;

private:
    IndexType type_;
    std::vector<std::string> columns_;
    
    std::unordered_map<std::string, std::vector<RowID>> unordered_map_;
    std::map<std::string, RowID> ordered_map_;
};

} 
} 

#endif // MEMDB_CORE_INDEX_H