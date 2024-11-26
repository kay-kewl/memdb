#include "memdb/core/DataType.h"

namespace memdb {
namespace core {

// ======= DataType Implementation =======

DataType::DataType(Type type) : type_(type), size_(0) {
    if ((type_ != Type::String && type_ != Type::Bytes) && size_ != 0) {
        throw std::invalid_argument("Size can only be set for String and Bytes types.");
    }
}

DataType::DataType(Type type, size_t size) : type_(type), size_(size) {
    if ((type_ != Type::String && type_ != Type::Bytes) || size_ == 0) {
        throw std::invalid_argument("Size can only be set for String and Bytes types with size > 0.");
    }
}

Type DataType::get_type() const {
    return type_;
}

size_t DataType::get_size() const {
    if (!is_string() && !is_bytes()) {
        throw std::logic_error("Only String and Bytes types have size.");
    }
    return size_;
}

bool DataType::is_string() const {
    return type_ == Type::String;
}

bool DataType::is_bytes() const {
    return type_ == Type::Bytes;
}

bool DataType::is_int32() const {
    return type_ == Type::Int32;
}

bool DataType::is_bool() const {
    return type_ == Type::Bool;
}

std::string DataType::to_string() const {
    switch (type_) {
        case Type::Int32:
            return "int32";
        case Type::Bool:
            return "bool";
        case Type::String:
            return "string[" + std::to_string(size_) + "]";
        case Type::Bytes:
            return "bytes[" + std::to_string(size_) + "]";
        default:
            return "unknown";
    }
}

} // namespace core
} // namespace memdb