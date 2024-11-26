#ifndef MEMDB_CORE_VALUE_H
#define MEMDB_CORE_VALUE_H

#include "memdb/core/enums/Type.h"

#include <string>
#include <variant>
#include <vector>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <iostream>

namespace memdb {
namespace core {

class Value {
public:
    Value() = default;
    Value(int32_t int_val);
    Value(bool bool_val);
    Value(const std::string& str_val);
    Value(const std::vector<uint8_t>& bytes_val);

    Type get_type() const;
    bool has_value() const;

    int32_t get_int() const;
    bool get_bool() const;
    const std::string& get_string() const;
    const std::vector<uint8_t>& get_bytes() const;

    const std::variant<int32_t, bool, std::string, std::vector<uint8_t>>& get_variant() const;
    std::variant<int32_t, bool, std::string, std::vector<uint8_t>>& get_variant();

    void set_int(int32_t int_val);
    void set_bool(bool bool_val);
    void set_string(const std::string& str_val);
    void set_bytes(const std::vector<uint8_t>& bytes_val);

    std::string to_string() const;

private:
    std::optional<std::variant<int32_t, bool, std::string, std::vector<uint8_t>>> data_;
};

} // namespace core
} // namespace memdb

#endif // MEMDB_CORE_VALUE_H