#ifndef MEMDB_CORE_EXPRESSION_H
#define MEMDB_CORE_EXPRESSION_H

#include "memdb/core/Value.h"
#include "memdb/core/DataType.h"

#include <memory>
#include <string>
#include <vector>
#include <unordered_map>

namespace memdb {
namespace core {

class Expression {
public:
    virtual ~Expression() = default;
    virtual DataType get_type() const = 0;
    virtual Value evaluate(const std::unordered_map<std::string, Value>& row) const = 0;
    virtual std::string to_string() const = 0;
};

class LiteralExpression : public Expression {
public:
    LiteralExpression(const Value& value);
    DataType get_type() const override;
    Value evaluate(const std::unordered_map<std::string, Value>& row) const override;
    std::string to_string() const override;

private:
    Value value_;
};

class VariableExpression : public Expression {
public:
    VariableExpression(const std::string& name);
    DataType get_type() const override;
    Value evaluate(const std::unordered_map<std::string, Value>& row) const override;
    std::string to_string() const override;

    const std::string& get_name() const { return name_; }

private:
    std::string name_;
};

class UnaryExpression : public Expression {
public:
    enum class Operator {
        Not,
        Length
    };

    UnaryExpression(Operator op, std::unique_ptr<Expression> operand);
    DataType get_type() const override;
    Value evaluate(const std::unordered_map<std::string, Value>& row) const override;
    std::string to_string() const override;

private:
    Operator op_;
    std::unique_ptr<Expression> operand_;
};

class BinaryExpression : public Expression {
public:
    enum class Operator {
        Add,
        Subtract,
        Multiply,
        Divide,
        Modulo,
        Less,
        LessEqual,
        Greater,
        GreaterEqual,
        Equal,
        NotEqual,
        And,
        Or,
        Xor
    };

    BinaryExpression(Operator op, std::unique_ptr<Expression> left, std::unique_ptr<Expression> right);
    DataType get_type() const override;
    Value evaluate(const std::unordered_map<std::string, Value>& row) const override;
    std::string to_string() const override;

    Operator get_operator() const { return op_; }
    const Expression* get_left() const { return left_.get(); }
    const Expression* get_right() const { return right_.get(); }

private:
    Operator op_;
    std::unique_ptr<Expression> left_;
    std::unique_ptr<Expression> right_;
};

} 
}

#endif // MEMDB_CORE_EXPRESSION_H