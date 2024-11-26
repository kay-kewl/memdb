#ifndef MEMDB_CORE_QUERYPARSER_H
#define MEMDB_CORE_QUERYPARSER_H

#include "memdb/core/Column.h"

#include "memdb/core/structs/ParsedQuery.h"

#include <string>
#include <memory>

namespace memdb {
namespace core {

class QueryParser {
public:
    ParsedQuery parse(const std::string& query);

private:
    Column parse_column_definition(const std::string& col_def);
    std::vector<std::string> split_columns(const std::string& columns_def);
    std::string trim(const std::string& s);
};

} // namespace core
} // namespace memdb

#endif // MEMDB_CORE_QUERYPARSER_H