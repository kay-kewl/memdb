#include "memdb/core/Expression.h"
#include "memdb/core/exceptions/TypeMismatchException.h"

#include <stdexcept>
#include <cmath>

namespace memdb {
namespace core {

int compare_values(const Value& left_val, const Value& right_val) {
    if (left_val.get_type() != right_val.get_type()) {
        throw exceptions::TypeMismatchException("Comparison requires operands of the same type.");
    }

    switch (left_val.get_type()) {
        case Type::Int32:
            return (left_val.get_int() < right_val.get_int()) ? -1 :
                   (left_val.get_int() > right_val.get_int()) ? 1 : 0;

        case Type::Bool:
            return (left_val.get_bool() == right_val.get_bool()) ? 0 :
                   (left_val.get_bool() == false) ? -1 : 1;

        case Type::String:
            return left_val.get_string().compare(right_val.get_string());

        case Type::Bytes:
            if (left_val.get_bytes() == right_val.get_bytes()) {
                return 0;
            } else if (left_val.get_bytes() < right_val.get_bytes()) {
                return -1;
            } else {
                return 1;
            }

        default:
            throw std::runtime_error("Unsupported type for comparison operations.");
    }
}

// ======= LiteralExpression Implementation =======
LiteralExpression::LiteralExpression(const Value& value) : value_(value) {}

Value LiteralExpression::evaluate(const std::unordered_map<std::string, Value>& row) const {
    return value_;
}

Type LiteralExpression::get_type() const {
    return value_.get_type();
}

// ======= VariableExpression Implementation =======
VariableExpression::VariableExpression(const std::string& name) : name_(name) {}

Value VariableExpression::evaluate(const std::unordered_map<std::string, Value>& row) const {
    auto it = row.find(name_);
    if (it == row.end()) {
        throw exceptions::TypeMismatchException("Column not found: " + name_);
    }
    if (!it->second.has_value()) {
        throw exceptions::TypeMismatchException("NULL value for column: " + name_);
    }
    return it->second;
}

Type VariableExpression::get_type() const {
    return Type::Unknown;
}

// ======= UnaryExpression Implementation =======
UnaryExpression::UnaryExpression(Operator op, std::unique_ptr<Expression> operand)
    : op_(op), operand_(std::move(operand)) {}

Value UnaryExpression::evaluate(const std::unordered_map<std::string, Value>& row) const {
    Value val = operand_->evaluate(row);
    switch (op_) {
        case Operator::Not:
            if (val.get_type() != Type::Bool) {
                throw exceptions::TypeMismatchException("Operator '!' requires Bool type.");
            }
            return Value(!val.get_bool());
        case Operator::Length:
            if (val.get_type() == Type::String) {
                return Value(static_cast<int32_t>(val.get_string().size()));
            } else if (val.get_type() == Type::Bytes) {
                return Value(static_cast<int32_t>(val.get_bytes().size()));
            } else {
                throw exceptions::TypeMismatchException("Operator '|var|' requires String or Bytes type.");
            }
        default:
            throw std::runtime_error("Unknown unary operator.");
    }
}

Type UnaryExpression::get_type() const {
    switch (op_) {
        case Operator::Not:
            return Type::Bool;
        case Operator::Length:
            return Type::Int32;
        default:
            return Type::Unknown;
    }
}

// ======= BinaryExpression Implementation =======
BinaryExpression::BinaryExpression(Operator op, std::unique_ptr<Expression> left, std::unique_ptr<Expression> right)
    : op_(op), left_(std::move(left)), right_(std::move(right)) {}

Value BinaryExpression::evaluate(const std::unordered_map<std::string, Value>& row) const {
    Value left_val = left_->evaluate(row);
    Value right_val = right_->evaluate(row);

    switch (op_) {
        case Operator::Add:
            if (left_val.get_type() == Type::Int32 && right_val.get_type() == Type::Int32) {
                return Value(left_val.get_int() + right_val.get_int());
            }
            if (left_val.get_type() == Type::String && right_val.get_type() == Type::String) {
                return Value(left_val.get_string() + right_val.get_string());
            }
            throw exceptions::TypeMismatchException("Operator '+' not supported for given types.");

        case Operator::Subtract:
            if (left_val.get_type() == Type::Int32 && right_val.get_type() == Type::Int32) {
                return Value(left_val.get_int() - right_val.get_int());
            }
            throw exceptions::TypeMismatchException("Operator '-' requires numeric types.");

        case Operator::Multiply:
            if (left_val.get_type() == Type::Int32 && right_val.get_type() == Type::Int32) {
                return Value(left_val.get_int() * right_val.get_int());
            }
            throw exceptions::TypeMismatchException("Operator '*' requires numeric types.");

        case Operator::Divide:
            if (left_val.get_type() == Type::Int32 && right_val.get_type() == Type::Int32) {
                if (right_val.get_int() == 0) throw exceptions::TypeMismatchException("Division by zero.");
                return Value(left_val.get_int() / right_val.get_int());
            }
            throw exceptions::TypeMismatchException("Operator '/' requires numeric types.");

        case Operator::Modulo:
            if (left_val.get_type() == Type::Int32 && right_val.get_type() == Type::Int32) {
                if (right_val.get_int() == 0) throw exceptions::TypeMismatchException("Modulo by zero.");
                return Value(left_val.get_int() % right_val.get_int());
            }
            throw exceptions::TypeMismatchException("Operator '%' requires integer types.");

                case Operator::Less:
            if (left_val.get_type() != right_val.get_type()) {
                throw exceptions::TypeMismatchException("Comparison requires operands of the same type.");
            }
            return Value(compare_values(left_val, right_val) < 0);

        case Operator::LessEqual:
            if (left_val.get_type() != right_val.get_type()) {
                throw exceptions::TypeMismatchException("Comparison requires operands of the same type.");
            }
            return Value(compare_values(left_val, right_val) <= 0);

        case Operator::Greater:
            if (left_val.get_type() != right_val.get_type()) {
                throw exceptions::TypeMismatchException("Comparison requires operands of the same type.");
            }
            return Value(compare_values(left_val, right_val) > 0);

        case Operator::GreaterEqual:
            if (left_val.get_type() != right_val.get_type()) {
                throw exceptions::TypeMismatchException("Comparison requires operands of the same type.");
            }
            return Value(compare_values(left_val, right_val) >= 0);

        case Operator::Equal:
            if (left_val.get_type() != right_val.get_type()) {
                throw exceptions::TypeMismatchException("Equality comparison requires operands of the same type.");
            }
            return Value(compare_values(left_val, right_val) == 0);

        case Operator::NotEqual:
            if (left_val.get_type() != right_val.get_type()) {
                throw exceptions::TypeMismatchException("Inequality comparison requires operands of the same type.");
            }
            return Value(compare_values(left_val, right_val) != 0);

        case Operator::And:
            if (left_val.get_type() != Type::Bool || right_val.get_type() != Type::Bool) {
                throw exceptions::TypeMismatchException("Operator '&&' requires Bool types.");
            }
            return Value(left_val.get_bool() && right_val.get_bool());

        case Operator::Or:
            if (left_val.get_type() != Type::Bool || right_val.get_type() != Type::Bool) {
                throw exceptions::TypeMismatchException("Operator '||' requires Bool types.");
            }
            return Value(left_val.get_bool() || right_val.get_bool());

        case Operator::Xor:
            if (left_val.get_type() != Type::Bool || right_val.get_type() != Type::Bool) {
                throw exceptions::TypeMismatchException("Operator '^^' requires Bool types.");
            }
            return Value(static_cast<bool>(left_val.get_bool() ^ right_val.get_bool()));

        default:
            throw std::runtime_error("Unknown binary operator.");
    }
}

Type BinaryExpression::get_type() const {
    switch (op_) {
        case Operator::Add:
        case Operator::Subtract:
        case Operator::Multiply:
        case Operator::Divide:
        case Operator::Modulo:
            return Type::Int32;
        case Operator::Less:
        case Operator::LessEqual:
        case Operator::Greater:
        case Operator::GreaterEqual:
        case Operator::Equal:
        case Operator::NotEqual:
        case Operator::And:
        case Operator::Or:
        case Operator::Xor:
            return Type::Bool;
        default:
            return Type::Unknown;
    }
}

} // namespace core
} // namespace memdb