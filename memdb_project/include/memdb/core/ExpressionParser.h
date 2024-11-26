#ifndef MEMDB_CORE_EXPRESSIONPARSER_H
#define MEMDB_CORE_EXPRESSIONPARSER_H

#include "memdb/core/Expression.h"
#include "memdb/core/Lexer.h"

#include <memory>

namespace memdb {
namespace core {

class ExpressionParser {
public:
    explicit ExpressionParser(const std::string& input);
    std::unique_ptr<Expression> parse_expression();

private:
    std::unique_ptr<Expression> parse_logical_or();
    std::unique_ptr<Expression> parse_logical_and();
    std::unique_ptr<Expression> parse_logical_xor();
    std::unique_ptr<Expression> parse_equality();
    std::unique_ptr<Expression> parse_comparison();
    std::unique_ptr<Expression> parse_addition();
    std::unique_ptr<Expression> parse_term();
    std::unique_ptr<Expression> parse_factor();
    std::unique_ptr<Expression> parse_unary();
    std::unique_ptr<Expression> parse_primary();

    Lexer lexer_;
    Token current_token_;

    void consume(TokenType type, const std::string& error_message);
    bool match(TokenType type, const std::string& value = "");
    void advance();
};

} // namespace core
} // namespace memdb

#endif // MEMDB_CORE_EXPRESSIONPARSER_H