#ifndef MEMDB_CORE_ENUMS_TOKENTYPE_H
#define MEMDB_CORE_ENUMS_TOKENTYPE_H

namespace memdb {
namespace core {

enum class TokenType {
    Identifier,
    IntLiteral,
    BoolLiteral,
    StringLiteral,
    BytesLiteral,
    Operator,
    Length,
    Comma,
    LeftParen,
    RightParen,
    EndOfInput
};

} 
} 

#endif // MEMDB_CORE_ENUMS_TOKENTYPE_H