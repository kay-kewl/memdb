#ifndef MEMDB_CORE_STRUCTS_PARSEDQUERY_H
#define MEMDB_CORE_STRUCTS_PARSEDQUERY_H

#include "memdb/core/Column.h"
#include "memdb/core/Value.h"
#include "memdb/core/Expression.h"

#include "memdb/core/structs/SelectItem.h"
#include "memdb/core/structs/JoinInfo.h"

#include <string>
#include <vector>
#include <optional>
#include <unordered_map>

namespace memdb {
namespace core {

struct ParsedQuery {
    enum class QueryType {
        CreateTable,
        Insert,
        Select,
        Update,
        Delete,
        CreateIndex,
    } type;

    std::string table_name;
    std::string table_alias;
    std::vector<Column> columns;

    std::optional<std::vector<std::optional<Value>>> insert_values;
    std::optional<std::unordered_map<std::string, Value>> insert_named_values;

    std::vector<SelectItem> select_items;
    
    std::unique_ptr<Expression> where_clause;

    std::unordered_map<std::string, std::unique_ptr<Expression>> update_assignments;
    std::unique_ptr<Expression> update_where_clause;

    std::unique_ptr<Expression> delete_where_clause;

    std::string from_table;

    std::string index_type;
    std::vector<std::string> index_columns;

    std::vector<JoinInfo> joins;

    // TODO: other fields if needed
};

} 
} 

#endif // MEMDB_CORE_STRUCTS_PARSEDQUERY_H