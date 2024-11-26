#ifndef MEMDB_CORE_EXCEPTIONS_DATABASEEXCEPTION_H
#define MEMDB_CORE_EXCEPTIONS_DATABASEEXCEPTION_H

#include <stdexcept>
#include <string>

namespace memdb {
namespace core {
namespace exceptions {

class DatabaseException : public std::runtime_error {
public:
    explicit DatabaseException(const std::string& message)
        : std::runtime_error(message) {}
};

class TableNotFoundException : public DatabaseException {
public:
    explicit TableNotFoundException(const std::string& table_name)
        : DatabaseException("Table not found: " + table_name) {}
};

class TableAlreadyExistsException : public DatabaseException {
public:
    explicit TableAlreadyExistsException(const std::string& table_name)
        : DatabaseException("Table already exists: " + table_name) {}
};

class RowNotFoundException : public DatabaseException {
public:
    explicit RowNotFoundException(core::RowID row_id)
        : DatabaseException("Row ID not found: " + std::to_string(row_id)) {}
};

class SerializationException : public DatabaseException {
public:
    explicit SerializationException(const std::string& message)
        : DatabaseException("Serialization Error: " + message) {}
};

} // namespace exceptions
} // namespace core
} // namespace memdb

#endif // MEMDB_CORE_EXCEPTIONS_DATABASEEXCEPTION_H