#include "memdb/core/QueryResult.h"

#include <algorithm>
#include <iostream>
#include <iomanip>

namespace memdb {
namespace core {

void QueryResult::print(std::ostream& os) const {
    if (!success_) {
        os << "Error: " << error_message_ << "\n";
        return;
    }

    os << "Number of columns: " << columns_.size() << "\n";

    std::vector<size_t> col_widths;
    for (size_t i = 0; i < columns_.size(); ++i) {
        size_t name_length = columns_[i].get_name().length();
        size_t type_length = columns_[i].type.to_string().length();
        size_t max_width = std::max(name_length, type_length);
        
        for (const auto& row : data_) {
            if (i < row.size()) { 
                const auto& val = row[i];
                size_t val_length = val.has_value() ? val->to_string().length() : 4;
                if (val_length > max_width) {
                    max_width = val_length;
                }
            }
        }
        col_widths.push_back(max_width + 2); 
    }

    auto print_separator = [&](const std::vector<size_t>& widths) {
        for (const auto& width : widths) {
            os << "+" << std::string(width + 2, '-');
        }
        os << "+\n";
    };

    print_separator(col_widths);

    os << "|";
    for (size_t i = 0; i < columns_.size(); ++i) {
        os << " " << std::left << std::setw(col_widths[i]) 
           << columns_[i].get_name() << " |";
    }
    os << "\n";

    os << "|";
    for (size_t i = 0; i < columns_.size(); ++i) {
        os << " " << std::left << std::setw(col_widths[i]) 
           << columns_[i].type.to_string() << " |";
    }
    os << "\n";

    print_separator(col_widths);

    for (const auto& row : data_) {
        os << "|";
        for (size_t i = 0; i < columns_.size(); ++i) {
            std::string value_str = "NULL";
            if (i < row.size() && row[i].has_value()) {
                value_str = row[i]->to_string();
            }
            if (value_str.length() > col_widths[i]) {
                value_str = value_str.substr(0, col_widths[i] - 3) + "...";
            }
            os << " " << std::left << std::setw(col_widths[i]) 
               << value_str << " |";
        }
        os << "\n";
    }

    print_separator(col_widths);
}

std::ostream& operator<<(std::ostream& os, const QueryResult& qr) {
    qr.print(os);
    return os;
}


QueryResultIterator QueryResult::begin() const {
    return QueryResultIterator(data_.cbegin(), columns_);
}

QueryResultIterator QueryResult::end() const {
    return QueryResultIterator(data_.cend(), columns_);
}

std::string QueryResult::to_string() const {
    if (!success_) return "Error: " + error_message_;
    
    std::ostringstream ss;
    std::vector<size_t> col_widths(columns_.size());

    for (size_t i = 0; i < columns_.size(); ++i) {
        col_widths[i] = std::max(
            columns_[i].name.length() + type_to_string(columns_[i].get_type()).length() + 3,
            get_max_value_width(i)
        );
    }

    print_separator(ss, col_widths);
    for (size_t i = 0; i < columns_.size(); ++i) {
        ss << "| " << std::left << std::setw(col_widths[i] - 2) 
           << columns_[i].name << " ";
    }
    ss << "|\n";

    for (size_t i = 0; i < columns_.size(); ++i) {
        ss << "| " << std::left << std::setw(col_widths[i] - 2) 
           << type_to_string(columns_[i].get_type()) << " ";
    }
    ss << "|\n";
    
    print_separator(ss, col_widths);

    for (const auto& row : data_) {
        for (size_t i = 0; i < row.size(); ++i) {
            ss << "| " << std::left << std::setw(col_widths[i] - 2)
               << (row[i].has_value() ? row[i]->to_string() : "NULL") << " ";
        }
        ss << "|\n";
    }
    
    print_separator(ss, col_widths);
    return ss.str();
}

void QueryResult::print_separator(std::ostringstream& ss, const std::vector<size_t>& widths) const {
    for (size_t width : widths) {
        ss << "+" << std::string(width, '-');
    }
    ss << "+\n";
}

size_t QueryResult::get_max_value_width(size_t col_idx) const {
    size_t max_width = 4; 
    for (const auto& row : data_) {
        if (row[col_idx].has_value()) {
            max_width = std::max(max_width, row[col_idx]->to_string().length());
        }
    }
    return max_width + 2;
}

std::string QueryResult::type_to_string(Type type) {
    switch (type) {
        case Type::Int32: return "int32";
        case Type::String: return "string";
        case Type::Bool: return "bool";
        case Type::Bytes: return "bytes";
        default: return "unknown";
    }
}

} 
} 