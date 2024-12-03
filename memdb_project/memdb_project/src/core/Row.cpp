#include "memdb/core/Row.h"

namespace memdb {
namespace core {

Row::Row(RowID id, const std::vector<std::optional<Value>>& values) : id_(id), values_(values) {}

RowID Row::get_id() const {
    return id_;
}

std::vector<std::optional<Value>>& Row::get_values() {
    return values_;
}

const std::vector<std::optional<Value>>& Row::get_values() const {
    return values_;
}

std::optional<Value>& Row::get_value(size_t index) {
    if (index >= values_.size()) {
        throw std::out_of_range("Column index out of range.");
    }
    return values_[index];
}

const std::optional<Value>& Row::get_value(size_t index) const {
    if (index >= values_.size()) {
        throw std::out_of_range("Column index out of range.");
    }
    return values_[index];
}

void Row::set_value(size_t index, const Value& value) {
    if (index >= values_.size()) {
        throw std::out_of_range("Column index out of range.");
    }
    values_[index] = value;
}

}
}