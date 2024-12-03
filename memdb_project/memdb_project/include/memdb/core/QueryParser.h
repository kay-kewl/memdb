#ifndef MEMDB_CORE_QUERYPARSER_H
#define MEMDB_CORE_QUERYPARSER_H

#include "memdb/core/Column.h"

#include "memdb/core/structs/ParsedQuery.h"

#include <string>
#include <memory>

namespace memdb {
namespace core {

class Database;

class QueryParser {
public:
    ParsedQuery parse(const std::string& query);
    bool validate_query(const std::string& query);

    void set_database(Database* db);

private:
    Database* db_ = nullptr;
    Column parse_column_definition(const std::string& col_def);
    std::vector<std::string> split_columns(const std::string& columns_def);
    std::string trim(const std::string& s);
};

} 
} 

#endif // MEMDB_CORE_QUERYPARSER_H