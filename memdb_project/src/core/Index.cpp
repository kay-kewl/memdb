#include "memdb/core/Index.h"

#include "memdb/core/exceptions/DatabaseException.h"

#include <algorithm>

namespace memdb {
namespace core {

// ======= Index Implementation =======

Index::Index(IndexType type, const std::vector<std::string>& columns)
    : type_(type), columns_(columns) {}

void Index::add_row(RowID row_id, const std::unordered_map<std::string, Value>& row) {
    if (type_ == IndexType::Unordered) {
        std::string key;
        for (const auto& col : columns_) {
            if (row.find(col) == row.end()) {
                throw std::invalid_argument("Column '" + col + "' not found in row for index.");
            }
            key += row.at(col).to_string() + "|";
        }
        unordered_map_[key].push_back(row_id);
    }
    else if (type_ == IndexType::Ordered) {
        if (columns_.size() != 1) {
            throw std::invalid_argument("Ordered index can only be created on a single column.");
        }
        const std::string& col = columns_[0];
        if (row.find(col) == row.end()) {
            throw std::invalid_argument("Column '" + col + "' not found in row for index.");
        }
        ordered_map_[row.at(col).to_string()] = row_id;
    }
}

void Index::remove_row(RowID row_id, const std::unordered_map<std::string, Value>& row) {
    if (type_ == IndexType::Unordered) {
        std::string key;
        for (const auto& col : columns_) {
            if (row.find(col) == row.end()) {
                throw std::invalid_argument("Column '" + col + "' not found in row for index.");
            }
            key += row.at(col).to_string() + "|";
        }
        auto it = unordered_map_.find(key);
        if (it != unordered_map_.end()) {
            it->second.erase(std::remove(it->second.begin(), it->second.end(), row_id), it->second.end());
            if (it->second.empty()) {
                unordered_map_.erase(it);
            }
        }
    }
    else if (type_ == IndexType::Ordered) {
        if (columns_.size() != 1) {
            throw std::invalid_argument("Ordered index can only be created on a single column.");
        }
        const std::string& col = columns_[0];
        auto it = ordered_map_.find(row.at(col).to_string());
        if (it != ordered_map_.end() && it->second == row_id) {
            ordered_map_.erase(it);
        }
    }
}

std::vector<RowID> Index::search_unordered(const std::unordered_map<std::string, Value>& condition) const {
    std::vector<RowID> result;
    std::string key;
    for (const auto& col : columns_) {
        auto it = condition.find(col);
        if (it == condition.end()) {
            return result;
        }
        key += it->second.to_string() + "|";
    }
    auto it = unordered_map_.find(key);
    if (it != unordered_map_.end()) {
        result = it->second;
    }
    return result;
}

std::vector<RowID> Index::search_ordered(const std::string& column,
                                        const std::optional<Value>& lower,
                                        bool lower_inclusive,
                                        const std::optional<Value>& upper,
                                        bool upper_inclusive) const {
    std::vector<RowID> result;
    if (columns_.size() != 1 || columns_[0] != column) {
        return result;
    }

    auto it_lower = lower.has_value() ? (lower_inclusive ? ordered_map_.lower_bound(lower->to_string()) 
                                                       : ordered_map_.upper_bound(lower->to_string()))
                                       : ordered_map_.begin();
    auto it_upper = upper.has_value() ? (upper_inclusive ? ordered_map_.upper_bound(upper->to_string()) 
                                                       : ordered_map_.lower_bound(upper->to_string()))
                                       : ordered_map_.end();

    for (auto it = it_lower; it != it_upper; ++it) {
        result.push_back(it->second);
    }

    return result;
}

} // namespace core
} // namespace memdb