#include "memdb/core/Value.h"

#include "memdb/core/exceptions/TypeMismatchException.h"

namespace memdb {
namespace core {

Value::Value(int32_t int_val) 
    : data_(int_val) {}

Value::Value(bool bool_val) 
    : data_(bool_val) {}

Value::Value(const std::string& str_val) 
    : data_(str_val) {}

Value::Value(const std::vector<uint8_t>& bytes_val) 
    : data_(bytes_val) {}

Type Value::get_type() const {
    if (!data_) {
        return Type::Unknown;
    }
    return std::visit([](auto&& arg) -> Type {
        using T = std::decay_t<decltype(arg)>;
        if constexpr (std::is_same_v<T, int32_t>) {
            return Type::Int32;
        } else if constexpr (std::is_same_v<T, bool>) {
            return Type::Bool;
        } else if constexpr (std::is_same_v<T, std::string>) {
            return Type::String;
        } else if constexpr (std::is_same_v<T, std::vector<uint8_t>>) {
            return Type::Bytes;
        } else {
            return Type::Unknown;
        }
    }, *data_);
}

bool Value::has_value() const {
    return data_.has_value();
}

int32_t Value::get_int() const {
    if (!data_ || !std::holds_alternative<int32_t>(*data_)) {
        throw exceptions::TypeMismatchException("Value is not of type Int32.");
    }
    return std::get<int32_t>(*data_);
}

bool Value::get_bool() const {
    if (!data_ || !std::holds_alternative<bool>(*data_)) {
        throw exceptions::TypeMismatchException("Value is not of type Bool.");
    }
    return std::get<bool>(*data_);
}

const std::string& Value::get_string() const {
    if (!data_ || !std::holds_alternative<std::string>(*data_)) {
        throw exceptions::TypeMismatchException("Value is not of type String.");
    }
    return std::get<std::string>(*data_);
}

const std::vector<uint8_t>& Value::get_bytes() const {
    if (!data_ || !std::holds_alternative<std::vector<uint8_t>>(*data_)) {
        throw exceptions::TypeMismatchException("Value is not of type Bytes.");
    }
    return std::get<std::vector<uint8_t>>(*data_);
}

const std::variant<int32_t, bool, std::string, std::vector<uint8_t>>& Value::get_variant() const {
    if (!data_) {
        throw exceptions::TypeMismatchException("Attempted to get variant of NULL Value.");
    }
    return *data_;
}

std::variant<int32_t, bool, std::string, std::vector<uint8_t>>& Value::get_variant() {
    if (!data_) {
        throw exceptions::TypeMismatchException("Attempted to get variant of NULL Value.");
    }
    return *data_;
}

void Value::set_int(int32_t int_val) {
    if (data_ && !std::holds_alternative<int32_t>(*data_)) {
        throw exceptions::TypeMismatchException("Cannot set Int32 on Value of different type.");
    }
    data_ = int_val;
}

void Value::set_bool(bool bool_val) {
    if (data_ && !std::holds_alternative<bool>(*data_)) {
        throw exceptions::TypeMismatchException("Cannot set Bool on Value of different type.");
    }
    data_ = bool_val;
}

void Value::set_string(const std::string& str_val) {
    if (data_ && !std::holds_alternative<std::string>(*data_)) {
        throw exceptions::TypeMismatchException("Cannot set String on Value of different type.");
    }
    data_ = str_val;
}

void Value::set_bytes(const std::vector<uint8_t>& bytes_val) {
    if (data_ && !std::holds_alternative<std::vector<uint8_t>>(*data_)) {
        throw exceptions::TypeMismatchException("Cannot set Bytes on Value of different type.");
    }
    data_ = bytes_val;
}

std::string Value::to_string() const {
    if (!data_) {
        return "NULL";
    }

    switch (data_->index()) {
        case 0:
            return std::to_string(std::get<int32_t>(*data_));
        case 1:
            return std::get<bool>(*data_) ? "true" : "false";
        case 2: 
            return "\"" + std::get<std::string>(*data_) + "\"";
        case 3:
        {
            const auto& bytes = std::get<std::vector<uint8_t>>(*data_);
            std::string hex_str = "0x";
            const char hex_chars[] = "0123456789ABCDEF";
            for (uint8_t byte : bytes) {
                hex_str += hex_chars[(byte >> 4) & 0xF];
                hex_str += hex_chars[byte & 0xF];
            }
            return hex_str;
        }
        default:
            return "unknown";
    }
}

}
}