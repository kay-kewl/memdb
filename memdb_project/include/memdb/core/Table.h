#ifndef MEMDB_CORE_TABLE_H
#define MEMDB_CORE_TABLE_H

#include "memdb/core/Column.h"
#include "memdb/core/DataType.h"
#include "memdb/core/Row.h"
#include "memdb/core/Value.h"
#include "memdb/core/Index.h"
#include "memdb/core/Expression.h"

#include <string>
#include <vector>
#include <unordered_map>
#include <optional>
#include <memory>
#include <stdexcept>
#include <cstdint>
#include <json.hpp>


namespace memdb {
namespace core {

class Table {
public:
    Table(const std::string& name, const std::vector<Column>& columns);

    const std::string& get_name() const;
    const std::vector<Column>& get_columns() const;
    size_t get_column_index(const std::string& column_name) const;

    void add_column(const Column& column);
    bool has_column(const std::string& column_name) const;

    RowID insert_row(const std::vector<std::optional<Value>>& values);
    RowID insert_row(const std::vector<std::optional<Value>>& values, RowID id);
    void delete_row(RowID id);
    Row& get_row(RowID id);
    const Row& get_row(RowID id) const;
    const std::unordered_map<RowID, Row>& get_all_rows() const;
    std::unordered_map<RowID, Row>& get_all_rows();
    const std::vector<std::unique_ptr<Index>>& get_indexes() const { return indexes_; }

    void add_index(const std::string& index_type_str, const std::vector<std::string>& columns);
    std::vector<RowID> find_rows(const std::unique_ptr<Expression>& condition) const;

    void validate_row(const std::vector<std::optional<Value>>& values) const;

    nlohmann::json to_json() const;
    static Table from_json(const nlohmann::json& j);

    std::string to_string() const;

private:
    std::string name_;
    std::vector<Column> columns_;
    std::unordered_map<RowID, Row> rows_;
    std::vector<std::unique_ptr<Index>> indexes_;
    RowID next_row_id_;
};

} // namespace core
} // namespace memdb

#endif // MEMDB_CORE_TABLE_H