#include "memdb/core/Column.h"

namespace memdb {
namespace core {

Column::Column(const std::string& name, const DataType& type,
               const std::vector<ColumnAttribute>& attributes,
               const std::optional<Value>& default_value)
    : name_(name), type_(type), attributes_(attributes), default_value_(default_value) {

    if (name.empty()) {
        throw std::invalid_argument("Column name cannot be empty.");
    }

    for (const auto& attr : attributes_) {
        if (attr == ColumnAttribute::AutoIncrement && !type_.is_int32()) {
            throw std::invalid_argument("AutoIncrement attribute can only be applied to int32 columns.");
        }
    }

    if (default_value_) {
        if (default_value_->get_type() != type_.get_type()) {
            throw std::invalid_argument("Default value type does not match column type.");
        }

        if (type_.is_string()) {
            if (default_value_->get_string().size() > type_.get_size()) {
                throw std::invalid_argument("Default string value exceeds defined size.");
            }
        } else if (type_.is_bytes()) {
            if (default_value_->get_bytes().size() > type_.get_size()) {
                throw std::invalid_argument("Default bytes value exceeds defined size.");
            }
        }
    }
}

const std::string& Column::get_name() const {
    return name_;
}

const DataType& Column::get_type() const {
    return type_;
}

const std::vector<ColumnAttribute>& Column::get_attributes() const {
    return attributes_;
}

const std::optional<Value>& Column::get_default_value() const {
    return default_value_;
}

bool Column::has_attribute(ColumnAttribute attribute) const {
    for (const auto& attr : attributes_) {
        if (attr == attribute) {
            return true;
        }
    }
    return false;
}

std::string Column::to_string() const {
    std::string attr_str;
    if (!attributes_.empty()) {
        attr_str += "{";
        for (size_t i = 0; i < attributes_.size(); ++i) {
            switch (attributes_[i]) {
                case ColumnAttribute::Unique:
                    attr_str += "unique";
                    break;
                case ColumnAttribute::AutoIncrement:
                    attr_str += "autoincrement";
                    break;
                case ColumnAttribute::Key:
                    attr_str += "key";
                    break;
                default:
                    attr_str += "unknown";
            }
            if (i < attributes_.size() - 1) {
                attr_str += ", ";
            }
        }
        attr_str += "} ";
    }

    std::string default_str = "";
    if (default_value_) {
        default_str = " = " + default_value_->to_string();
    }

    return attr_str + name_ + " : " + type_.to_string() + default_str;
}

} 
}