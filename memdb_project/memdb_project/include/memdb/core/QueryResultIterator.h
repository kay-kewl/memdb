#ifndef MEMDB_CORE_QUERYRESULTITERATOR_H
#define MEMDB_CORE_QUERYRESULTITERATOR_H

#include "memdb/core/ResultRow.h"
#include "memdb/core/structs/ColumnInfo.h"
#include <vector>

namespace memdb {
namespace core {

class QueryResultIterator {
public:
    using DataIterator = std::vector<std::vector<std::optional<Value>>>::const_iterator;

    QueryResultIterator(DataIterator data_iter, const std::vector<ColumnInfo>& columns)
                        : data_iter_(data_iter), columns_(&columns) {}

    ResultRow operator*() const;
    QueryResultIterator& operator++();
    bool operator!=(const QueryResultIterator& other) const;

private:
    DataIterator data_iter_;
    const std::vector<ColumnInfo>* columns_;
};

} 
} 

#endif // MEMDB_CORE_QUERYRESULTITERATOR_H