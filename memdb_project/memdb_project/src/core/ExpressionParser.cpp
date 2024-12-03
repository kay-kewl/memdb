#include "memdb/core/ExpressionParser.h"
#include "memdb/core/exceptions/TypeMismatchException.h"

#include <stdexcept>
#include <limits>
#include <numeric>

namespace memdb {
namespace core {

ExpressionParser::ExpressionParser(const std::string& input) : lexer_(input) {
    advance();
}

void ExpressionParser::advance() {
    current_token_ = lexer_.get_next_token();
}

void ExpressionParser::consume(TokenType type, const std::string& error_message) {
    if (current_token_.type != type) {
        throw exceptions::TypeMismatchException(error_message);
    }
    advance();
}

bool ExpressionParser::match(TokenType type, const std::string& value) {
    if (current_token_.type == type) {
        if (value.empty() || current_token_.value == value) {
            return true;
        }
    }
    return false;
}

std::unique_ptr<Expression> ExpressionParser::parse_expression() {
    auto expr = parse_logical_or();
    if (current_token_.type != TokenType::EndOfInput && !(current_token_.type == TokenType::Identifier && current_token_.value == "as") && current_token_.type != TokenType::RightParen) {
        throw exceptions::TypeMismatchException("Unexpected token in expression.");
    }
    return expr;
}

std::unique_ptr<Expression> ExpressionParser::parse_logical_or() {
    auto node = parse_logical_xor();
    while (match(TokenType::Operator, "||")) {
        std::string op = current_token_.value;
        advance();
        auto right = parse_logical_and();
        node = std::make_unique<BinaryExpression>(BinaryExpression::Operator::Or, std::move(node), std::move(right));
    }
    return node;
}

std::unique_ptr<Expression> ExpressionParser::parse_logical_xor() {
    auto node = parse_logical_and();
    while (match(TokenType::Operator, "^^")) {
        std::string op = current_token_.value;
        advance();
        auto right = parse_logical_and();
        node = std::make_unique<BinaryExpression>(BinaryExpression::Operator::Xor, std::move(node), std::move(right));
    }
    return node;
}

std::unique_ptr<Expression> ExpressionParser::parse_logical_and() {
    auto node = parse_equality();
    while (match(TokenType::Operator, "&&")) {
        std::string op = current_token_.value;
        advance();
        auto right = parse_equality();
        node = std::make_unique<BinaryExpression>(BinaryExpression::Operator::And, std::move(node), std::move(right));
    }
    return node;
}

std::unique_ptr<Expression> ExpressionParser::parse_equality() {
    auto node = parse_comparison();
    while (match(TokenType::Operator, "==") || match(TokenType::Operator, "!=") || match(TokenType::Operator, "=")) {
        std::string op = current_token_.value;
        advance();
        auto right = parse_comparison();
        if (op == "==" || op == "=") {
            node = std::make_unique<BinaryExpression>(BinaryExpression::Operator::Equal, std::move(node), std::move(right));
        } else if (op == "!=") {
            node = std::make_unique<BinaryExpression>(BinaryExpression::Operator::NotEqual, std::move(node), std::move(right));
        } else {
            throw std::runtime_error("Unknown equality operator: " + op);
        }
    }
    return node;
}

std::unique_ptr<Expression> ExpressionParser::parse_comparison() {
    auto node = parse_term();
    while (match(TokenType::Operator, "<") || match(TokenType::Operator, "<=") ||
           match(TokenType::Operator, ">") || match(TokenType::Operator, ">=")) {
        std::string op = current_token_.value;
        advance();
        auto right = parse_term();
        if (op == "<") {
            node = std::make_unique<BinaryExpression>(BinaryExpression::Operator::Less, std::move(node), std::move(right));
        } else if (op == "<=") {
            node = std::make_unique<BinaryExpression>(BinaryExpression::Operator::LessEqual, std::move(node), std::move(right));
        } else if (op == ">") {
            node = std::make_unique<BinaryExpression>(BinaryExpression::Operator::Greater, std::move(node), std::move(right));
        } else if (op == ">=") {
            node = std::make_unique<BinaryExpression>(BinaryExpression::Operator::GreaterEqual, std::move(node), std::move(right));
        }
    }
    return node;
}

std::unique_ptr<Expression> ExpressionParser::parse_term() {
    auto node = parse_factor();
    while (match(TokenType::Operator, "+") || match(TokenType::Operator, "-")) {
        std::string op = current_token_.value;
        advance();
        auto right = parse_factor();
        if (op == "+") {
            node = std::make_unique<BinaryExpression>(BinaryExpression::Operator::Add, std::move(node), std::move(right));
        } else {
            node = std::make_unique<BinaryExpression>(BinaryExpression::Operator::Subtract, std::move(node), std::move(right));
        }
    }
    return node;
}

std::unique_ptr<Expression> ExpressionParser::parse_factor() {
    auto node = parse_unary();
    while (match(TokenType::Operator, "*") || match(TokenType::Operator, "/") || match(TokenType::Operator, "%")) {
        std::string op = current_token_.value;
        advance();
        auto right = parse_unary();
        if (op == "*") {
            node = std::make_unique<BinaryExpression>(BinaryExpression::Operator::Multiply, std::move(node), std::move(right));
        } else if (op == "/") {
            node = std::make_unique<BinaryExpression>(BinaryExpression::Operator::Divide, std::move(node), std::move(right));
        } else if (op == "%") {
            node = std::make_unique<BinaryExpression>(BinaryExpression::Operator::Modulo, std::move(node), std::move(right));
        }
    }
    return node;
}

std::unique_ptr<Expression> ExpressionParser::parse_unary() {
    if (match(TokenType::Operator, "!")) {
        advance();
        auto operand = parse_unary();
        return std::make_unique<UnaryExpression>(UnaryExpression::Operator::Not, std::move(operand));
    }

    return parse_primary();
}

std::unique_ptr<Expression> ExpressionParser::parse_primary() {
    if (match(TokenType::Length)) {
        std::string var_name = current_token_.value;
        advance();

        auto var_expr = std::make_unique<VariableExpression>(var_name);

        return std::make_unique<UnaryExpression>(UnaryExpression::Operator::Length, std::move(var_expr));
    }

    if (match(TokenType::LeftParen)) {
        advance();
        auto expr = parse_expression();
        consume(TokenType::RightParen, "Expected ')' after expression.");
        return expr;
    }

    if (match(TokenType::IntLiteral)) {
        int32_t value = std::stoll(current_token_.value);
        advance();
        return std::make_unique<LiteralExpression>(Value(static_cast<int32_t>(value)));
    }

    if (match(TokenType::BoolLiteral)) {
        bool value = (current_token_.value == "true");
        advance();
        return std::make_unique<LiteralExpression>(Value(value));
    }

    if (match(TokenType::StringLiteral)) {
        std::string value = current_token_.value;
        advance();
        return std::make_unique<LiteralExpression>(Value(value));
    }

    if (match(TokenType::BytesLiteral)) {
        std::string hex_str = current_token_.value;
        advance();
        std::vector<uint8_t> bytes;
        if (hex_str.size() < 2 || (hex_str[0] != '0' && hex_str[1] != 'x' && hex_str[1] != 'X')) {
            throw exceptions::TypeMismatchException("Invalid bytes literal.");
        }
        hex_str = hex_str.substr(2);
        if (hex_str.size() % 2 != 0) {
            throw exceptions::TypeMismatchException("Invalid bytes literal length.");
        }
        for (size_t i = 0; i < hex_str.size(); i += 2) {
            std::string byte_str = hex_str.substr(i, 2);
            uint8_t byte = static_cast<uint8_t>(std::stoi(byte_str, nullptr, 16));
            bytes.push_back(byte);
        }
        return std::make_unique<LiteralExpression>(Value(bytes));
    }

    if (match(TokenType::Identifier)) {
        std::string name = current_token_.value;
        advance();
        return std::make_unique<VariableExpression>(name);
    }

    throw exceptions::TypeMismatchException("Unexpected token in expression.");
}

}
}