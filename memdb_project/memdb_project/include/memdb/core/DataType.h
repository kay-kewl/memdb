#ifndef MEMDB_CORE_DATATYPE_H
#define MEMDB_CORE_DATATYPE_H

#include "memdb/core/enums/Type.h"

#include <string>
#include <variant>
#include <vector>
#include <cstdint>
#include <stdexcept>
#include <iostream>

namespace memdb {
namespace core {

class DataType {
public:
    DataType(Type type);
    DataType(Type type, size_t size);

    Type get_type() const;
    size_t get_size() const; 

    bool is_string() const;
    bool is_bytes() const;
    bool is_int32() const;
    bool is_bool() const;

    std::string to_string() const;

private:
    Type type_;
    size_t size_;
};

} 
} 

#endif // MEMDB_CORE_DATATYPE_H