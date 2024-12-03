#ifndef MEMDB_CORE_QUERYRESULT_H
#define MEMDB_CORE_QUERYRESULT_H

#include "memdb/core/Value.h"
#include "memdb/core/QueryResultIterator.h"

#include "memdb/core/structs/ColumnInfo.h"

#include <vector>
#include <string>
#include <optional>
#include <iterator>

namespace memdb {
namespace core {

class QueryResult {
public:
    QueryResult() : success_(true) {}
    QueryResult(const std::vector<std::vector<std::optional<Value>>>& data) 
                                : success_(true), data_(data), columns_() {}
    QueryResult(const std::vector<std::vector<std::optional<Value>>>& data, 
                const std::vector<ColumnInfo>& columns) : success_(true), data_(data), columns_(columns) {}
    QueryResult(const std::string& error_message) : success_(false), error_message_(error_message) {}

    bool is_ok() const { return success_; }
    std::string get_error() const { return error_message_; }

    QueryResultIterator begin() const;
    QueryResultIterator end() const;

    const std::vector<std::vector<std::optional<Value>>>& get_data() const { return data_; }
    const std::vector<ColumnInfo>& get_columns() const { return columns_; }

    std::string to_string() const;
    void print(std::ostream& os = std::cout) const;

    friend std::ostream& operator<<(std::ostream& os, const QueryResult& result);

private:
    bool success_;
    std::string error_message_;
    std::vector<std::vector<std::optional<Value>>> data_;
    std::vector<ColumnInfo> columns_;

    void print_separator(std::ostringstream& ss, const std::vector<size_t>& widths) const;
    size_t get_max_value_width(size_t col_idx) const;
    static std::string type_to_string(Type type);
};

}
}

#endif // MEMDB_CORE_QUERYRESULT_H