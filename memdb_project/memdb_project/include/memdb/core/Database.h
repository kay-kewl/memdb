#ifndef MEMDB_CORE_DATABASE_H
#define MEMDB_CORE_DATABASE_H

#include "memdb/core/Table.h"
#include "memdb/core/Value.h"
#include "memdb/core/Column.h"
#include "memdb/core/Row.h"
#include "memdb/core/QueryParser.h"
#include "memdb/core/QueryExecutor.h"

#include "memdb/core/structs/ParsedQuery.h"

#include <string>
#include <unordered_map>
#include <memory>
#include <optional>
#include <vector>
#include <fstream>
#include <stdexcept>

namespace memdb {
namespace core {

class Database {
public:
    Database();

    void create_table(const std::string& table_name, const std::vector<Column>& columns);
    void drop_table(const std::string& table_name);
    std::shared_ptr<Table> get_table(const std::string& table_name) const;
    bool has_table(const std::string& table_name) const;

    RowID insert_row(const std::string& table_name, const std::vector<std::optional<Value>>& values);
    void delete_row(const std::string& table_name, core::RowID row_id);
    Row& get_row(const std::string& table_name, core::RowID row_id);
    const Row& get_row(const std::string& table_name, core::RowID row_id) const;

    const std::unordered_map<std::string, std::shared_ptr<Table>>& get_all_tables() const;
    void save_to_file(const std::string& filename) const;
    void load_from_file(const std::string& filename);

    QueryResult execute(const std::string& query);
    
    void create_index(const std::string& table_name, const std::string& index_type, const std::vector<std::string>& columns);

    std::string to_string() const;

private:
    std::unordered_map<std::string, std::shared_ptr<Table>> tables_;

    QueryParser parser_;
    QueryExecutor executor_;
};

} 
} 

#endif // MEMDB_CORE_DATABASE_H