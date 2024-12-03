#ifndef MEMDB_CORE_EXCEPTIONS_TYPEMISMATCHEXCEPTION_H
#define MEMDB_CORE_EXCEPTIONS_TYPEMISMATCHEXCEPTION_H

#include <stdexcept>
#include <string>

namespace memdb {
namespace core {
namespace exceptions {

class TypeMismatchException : public std::runtime_error {
public:
    explicit TypeMismatchException(const std::string& message)
        : std::runtime_error(message) {}
};

} 
} 
} 

#endif // MEMDB_CORE_EXCEPTIONS_TYPEMISMATCHEXCEPTION_H