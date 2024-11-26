#include "memdb/core/Lexer.h"

#include "memdb/core/exceptions/TypeMismatchException.h"

#include <limits>
#include <numeric>
#include <iostream>

namespace memdb {
namespace core {

Lexer::Lexer(const std::string& input) : input_(input), pos_(0) {}

char Lexer::peek() const {
    if (pos_ >= input_.size()) return '\0';
    return input_[pos_];
}

char Lexer::get_char() {
    if (pos_ >= input_.size()) return '\0';
    return input_[pos_++];
}

void Lexer::skip_whitespace() {
    while (std::isspace(peek())) {
        get_char();
    }
}

Token Lexer::get_next_token() {
    skip_whitespace();

    char current = peek();

    if (current == '\0') {
        return Token{TokenType::EndOfInput, ""};
    }

    if (std::isalpha(current) || current == '_') {
        return identifier();
    }

    if (current == '0' && (input_.size() > pos_ + 1) && (input_[pos_ + 1] == 'x' || input_[pos_ + 1] == 'X')) {
        return bytes_literal();
    }

    if (std::isdigit(current) || 
        ((current == '-' || current == '+') && 
         (pos_ + 1 < input_.size()) && 
         std::isdigit(input_[pos_ + 1]))) {
        Token tok = number();
        return tok;
    }

    if (current == '"') {
        return string_literal();
    }

    std::string var_name;  // for Length operator

    switch (current) {
        case ',': get_char(); return Token{TokenType::Comma, ","};
        case '(': get_char(); return Token{TokenType::LeftParen, "("};
        case ')': get_char(); return Token{TokenType::RightParen, ")"};
        case '+': get_char(); return Token{TokenType::Operator, "+"};
        case '-': get_char(); return Token{TokenType::Operator, "-"};
        case '*': get_char(); return Token{TokenType::Operator, "*"};
        case '/': get_char(); return Token{TokenType::Operator, "/"};
        case '%': get_char(); return Token{TokenType::Operator, "%"};
        case '<':
            get_char();
            if (peek() == '=') {
                get_char();
                return Token{TokenType::Operator, "<="};
            }
            return Token{TokenType::Operator, "<"};
        case '>':
            get_char();
            if (peek() == '=') {
                get_char();
                return Token{TokenType::Operator, ">="};
            }
            return Token{TokenType::Operator, ">"};
        case '=':
            get_char();
            if (peek() == '=') {
                get_char();
                return Token{TokenType::Operator, "=="};
            }
            return Token{TokenType::Operator, "="};
        case '!':
            get_char();
            if (peek() == '=') {
                get_char();
                return Token{TokenType::Operator, "!="};
            }
            return Token{TokenType::Operator, "!"};
        case '&':
            get_char();
            if (peek() == '&') {
                get_char();
                return Token{TokenType::Operator, "&&"};
            }
            throw exceptions::TypeMismatchException("Invalid character after '&'");
        case '|':
            get_char();
            if (peek() == '|') {
                get_char();
                return Token{TokenType::Operator, "||"};
            }

            while (std::isalnum(peek()) || peek() == '_') {
                var_name += get_char();
            }

            if (peek() == '|') {
                get_char();
                return Token{TokenType::Length, var_name};
            }
            else {
                throw exceptions::TypeMismatchException("Invalid operator after '|': expected '|'");
            }
            return Token{TokenType::Operator, "|"};
        case '^':
            get_char();
            if (peek() == '^') {
                get_char();
                return Token{TokenType::Operator, "^^"};
            }
            throw exceptions::TypeMismatchException("Invalid character after '^'");
        default:
            throw exceptions::TypeMismatchException(std::string("Unknown character: ") + current);
    }
}

Token Lexer::identifier() {
    std::string result;
    while (std::isalnum(peek()) || peek() == '_' || peek() == '.') { // Include dot
        result += get_char();
    }

    if (result == "true" || result == "false") {
        return Token{TokenType::BoolLiteral, result};
    }

    return Token{TokenType::Identifier, result};
}

Token Lexer::number() {
    std::string num_str;
    if (peek() == '-' || peek() == '+') {
        num_str += get_char();
    }

    while (std::isdigit(peek())) {
        num_str += get_char();
    }

    return Token{TokenType::IntLiteral, num_str};
}

Token Lexer::string_literal() {
    std::string str;
    get_char(); // consume opening ".
    while (peek() != '"' && peek() != '\0') {
        if (peek() == '\\') { 
            // Handle escape sequences
            get_char(); // consume '\\'
            char esc = get_char();
            switch (esc) {
                case 'n': str += '\n'; break;
                case 't': str += '\t'; break;
                case 'r': str += '\r'; break;
                case '\\': str += '\\'; break;
                case '"': str += '"'; break;
                default: str += esc; break;
            }
        } else {
            str += get_char();
        }
    }
    if (peek() == '\0') {
        throw exceptions::TypeMismatchException("Unterminated string literal.");
    }
    get_char(); // consume closing "
    return Token{TokenType::StringLiteral, str};
}

Token Lexer::bytes_literal() {
    std::string bytes_str;
    bytes_str += get_char(); // consume '0'
    bytes_str += get_char(); // consume 'x' or 'X'

    while (std::isxdigit(peek())) {
        bytes_str += get_char();
    }

    return Token{TokenType::BytesLiteral, bytes_str};
}

} // namespace core
} // namespace memdb