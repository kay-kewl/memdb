#ifndef MEMDB_CORE_LEXER_H
#define MEMDB_CORE_LEXER_H

#include "memdb/core/structs/Token.h"

#include <string>
#include <vector>
#include <variant>
#include <cctype>

namespace memdb {
namespace core {

class Lexer {
public:
    explicit Lexer(const std::string& input);
    Token get_next_token();

private:
    char peek() const;
    char get_char();
    void skip_whitespace();
    Token identifier();
    Token number();
    Token string_literal();
    Token bytes_literal();
    
    std::string input_;
    size_t pos_;
};

} // namespace core
} // namespace memdb

#endif // MEMDB_CORE_LEXER_H