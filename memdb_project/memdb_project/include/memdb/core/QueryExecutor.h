#ifndef MEMDB_CORE_QUERYEXECUTOR_H
#define MEMDB_CORE_QUERYEXECUTOR_H

#include "memdb/core/Expression.h"
#include "memdb/core/QueryResult.h"

#include "memdb/core/structs/ParsedQuery.h"

#include <memory>

namespace memdb {
namespace core {

class Database;

class QueryExecutor {
public:
    QueryResult execute(const ParsedQuery& parsed_query, Database& db);
    
private:
    QueryResult execute_select(const ParsedQuery& pq, Database& db);
    QueryResult execute_update(const ParsedQuery& pq, Database& db);
    QueryResult execute_delete(const ParsedQuery& pq, Database& db);
    QueryResult execute_create_index(const ParsedQuery& pq, Database& db);
};

} 
} 

#endif // MEMDB_CORE_QUERYEXECUTOR_H