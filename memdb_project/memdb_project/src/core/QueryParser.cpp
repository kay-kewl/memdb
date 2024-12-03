#include "memdb/core/QueryParser.h"
#include "memdb/core/Table.h"
#include "memdb/core/Database.h"
#include "memdb/core/ExpressionParser.h"

#include "memdb/core/exceptions/DatabaseException.h"

#include <sstream>
#include <algorithm>
#include <cctype>
#include <limits>
#include <stdexcept>
#include <regex>
#include <unordered_set>

namespace memdb {
namespace core {

bool QueryParser::validate_query(const std::string& query) {
    std::stack<char> s;
    for (char ch : query) {
        if (ch == '(' || ch == '{') {
            s.push(ch);
        } else if (ch == ')' || ch == '}') {
            if (s.empty()) {
                return false;
            }
            char open = s.top();
            s.pop();
            if ((ch == ')' && open != '(') || (ch == '}' && open != '{')) {
                return false;
            }
        }
    }
    return s.empty();
}

const std::unordered_set<std::string> reservedKeywords = {
    "create", "table", "insert", "update", "delete", "join", "where", "int32", "string", "bytes", "bool", "key", "unique", "autoincrement",  "index", "unordered", "ordered", "on", "select", "from", "values", "as"
};

bool isValidIdentifier(const std::string& identifier) {
    std::regex valid_identifier_regex("^[a-zA-Z][a-zA-Z0-9_]*$");
    bool regex_match = std::regex_match(identifier, valid_identifier_regex);
    if (!regex_match) {
        return false;
    }
    
    std::string lower_identifier = identifier;
    std::transform(lower_identifier.begin(), lower_identifier.end(), lower_identifier.begin(), ::tolower);
    return reservedKeywords.find(lower_identifier) == reservedKeywords.end();
}

void QueryParser::set_database(Database* db) {
    db_ = db;
}

Value parse_value(TokenType token_type, const std::string& value_str, size_t size = 0) {
    switch (token_type) {
        case TokenType::StringLiteral:
            if (size > 0 && value_str.size() > size) {
                throw std::invalid_argument("String value exceeds defined size of " + 
                    std::to_string(size));
            }
            
            return Value(value_str);

        case TokenType::BytesLiteral: {
            std::string hex_str = value_str.substr(2);
            if (hex_str.size() % 2 != 0) {
                throw std::invalid_argument("Invalid hex string for bytes.");
            }
            size_t bytes_size = hex_str.size() / 2;
            if (bytes_size > size) {
                throw std::invalid_argument("Bytes value exceeds defined size of " + 
                    std::to_string(size));
            }
            std::vector<uint8_t> bytes;
            bytes.reserve(bytes_size);
            for (size_t i = 0; i < hex_str.size(); i += 2) {
                std::string byte_str = hex_str.substr(i, 2);
                uint8_t byte = static_cast<uint8_t>(std::stoul(byte_str, nullptr, 16));
                bytes.push_back(byte);
            }
            return Value(bytes);
        }

        case TokenType::BoolLiteral:
            return Value(value_str == "true");

        case TokenType::IntLiteral:
            try {
                int32_t int_val = std::stoi(value_str);
                return Value(int_val);
            } catch (const std::exception&) {
                throw std::invalid_argument("Invalid integer value: " + value_str);
            }

        default:
            throw std::invalid_argument("Invalid value: " + value_str);
    }
}

std::vector<std::optional<Value>> parse_values(const std::string& values_str, const std::vector<Column>& columns) {
    std::vector<std::optional<Value>> values(columns.size(), std::nullopt);
    Lexer lexer(values_str);
    Token token = lexer.get_next_token();

    bool named_format = false;
    size_t peek_pos = values_str.find('=');
    if (peek_pos != std::string::npos) {
        named_format = true;
    }

    if (named_format) {
        std::unordered_set<std::string> processed_columns;
        while (token.type != TokenType::EndOfInput) {
            std::string column_name;
            while (token.type != TokenType::EndOfInput && token.type != TokenType::Operator && token.value != "=") {
                if (token.type == TokenType::Identifier) {
                    column_name = token.value;
                }
                token = lexer.get_next_token();
            }
            
            if (token.type != TokenType::Operator || token.value != "=") {
                throw std::invalid_argument("Expected '=' after column name");
            }
            token = lexer.get_next_token();

            if (processed_columns.find(column_name) != processed_columns.end()) {
                throw std::invalid_argument("Duplicate column name: " + column_name);
            }
            processed_columns.insert(column_name);
            
            size_t col_idx = 0;
            bool found = false;
            for (size_t i = 0; i < columns.size(); i++) {
                if (columns[i].get_name() == column_name) {
                    col_idx = i;
                    found = true;
                    break;
                }
            }
            if (!found) {
                throw std::invalid_argument("Column not found: " + column_name);
            }
            
            if (token.type == TokenType::StringLiteral || token.type == TokenType::BytesLiteral ||
                token.type == TokenType::IntLiteral || token.type == TokenType::BoolLiteral) {
                const Column& col = columns[col_idx];
                size_t size = (col.get_type().get_type() == Type::String || 
                             col.get_type().get_type() == Type::Bytes) ? col.get_type().get_size() : 0;
                values[col_idx] = parse_value(token.type, token.value, size);
            }
            
            token = lexer.get_next_token();
            if (token.type == TokenType::Comma) {
                token = lexer.get_next_token();
            }
        }

        for (size_t i = 0; i < columns.size(); ++i) {
            if (!values[i].has_value()) {
                if (columns[i].has_attribute(ColumnAttribute::AutoIncrement)) {
                    continue;
                }
                if (columns[i].get_default_value().has_value()) {
                    values[i] = columns[i].get_default_value();
                } else {
                    throw std::invalid_argument("Missing value for column: " + columns[i].get_name());
                }
            }
        }
    } else {
        size_t column_index = 0;
        while (token.type != TokenType::EndOfInput) {
            if (token.type == TokenType::Comma) {
                values[column_index] = std::nullopt;
                token = lexer.get_next_token();
                column_index++;
                continue;
            }

            if (column_index >= columns.size()) {
                throw std::invalid_argument("Too many values for table columns");
            }

            const Column& current_column = columns[column_index];
            const DataType& dtype = current_column.get_type();
            size_t size = 0;

            if (dtype.get_type() == Type::String || dtype.get_type() == Type::Bytes) {
                size = dtype.get_size();
            }

            if (token.type == TokenType::StringLiteral || token.type == TokenType::BytesLiteral ||
                token.type == TokenType::IntLiteral || token.type == TokenType::BoolLiteral) {
                if (!token.value.empty() || token.type == TokenType::StringLiteral) {
                    values[column_index] = parse_value(token.type, token.value, size);
                } else {
                    values[column_index] = std::nullopt;
                }
            } else {
                throw std::invalid_argument("Invalid value in INSERT statement: " + token.value);
            }

            token = lexer.get_next_token();
            if (token.type == TokenType::Comma) {
                token = lexer.get_next_token();
            }
            column_index++;
        }

        while (column_index < columns.size()) {
            if (columns[column_index].has_attribute(ColumnAttribute::AutoIncrement)) {
                column_index++;
                continue;
            }
            if (columns[column_index].get_default_value().has_value()) {
                values[column_index] = columns[column_index].get_default_value();
            } else {
                throw std::invalid_argument("Missing value for column: " + columns[column_index].get_name());
            }
            column_index++;
        }
    }

    return values;
}

ParsedQuery QueryParser::parse(const std::string& query) {
    bool valid = QueryParser::validate_query(query);
    if (!valid) {
        throw std::invalid_argument("Unbalanced parentheses or braces in query.");
    }
    std::string trimmed_query = trim(query);

    if (!trimmed_query.empty() && trimmed_query.back() == ';') {
        trimmed_query.pop_back();
        trimmed_query = trim(trimmed_query);
    }

    ParsedQuery pq;
    std::istringstream iss(trimmed_query);
    std::string command;
    iss >> command;

    std::transform(command.begin(), command.end(), command.begin(), ::tolower);

    if (command == "create") {
        std::string index_type_or_subcommand;
        iss >> index_type_or_subcommand;
        std::transform(index_type_or_subcommand.begin(), index_type_or_subcommand.end(), index_type_or_subcommand.begin(), ::tolower);
        if (index_type_or_subcommand == "table") {
            pq.type = ParsedQuery::QueryType::CreateTable;

            iss >> std::ws;
            if (iss.peek() == '(') {
                throw std::invalid_argument("Table name cannot be empty.");
            }

            iss >> pq.table_name;

            if (!isValidIdentifier(pq.table_name)) {
                throw std::invalid_argument("Invalid table name: " + pq.table_name);
            }
            if (reservedKeywords.count(pq.table_name)) {
                throw std::invalid_argument("Reserved keyword used as table name: " + pq.table_name);
            }

            char c;
            iss >> c;
            if (c != '(') {
                throw std::invalid_argument("Expected '(' after table name.");
            }

            std::string columns_def;
            std::getline(iss, columns_def, ';');
            columns_def = columns_def.substr(0, columns_def.size() - 1);

            std::vector<std::string> column_defs = split_columns(columns_def);

            for (const auto& col_def : column_defs) {
                Column column = parse_column_definition(col_def);
                if (!isValidIdentifier(column.get_name())) {
                    throw std::invalid_argument("Invalid column name: " + column.get_name());
                }
                pq.columns.push_back(column);
            }
        }
        else if (index_type_or_subcommand == "ordered" || index_type_or_subcommand == "unordered") {
            pq.type = ParsedQuery::QueryType::CreateIndex;
            pq.index_type = index_type_or_subcommand;

            std::string index_keyword;
            iss >> index_keyword;
            std::transform(index_keyword.begin(), index_keyword.end(), index_keyword.begin(), ::tolower);
            if (index_keyword != "index") {
                throw std::invalid_argument("Expected 'index' after index type.");
            }

            std::string on_word;
            iss >> on_word;
            std::transform(on_word.begin(), on_word.end(), on_word.begin(), ::tolower);
            if (on_word != "on") {
                throw std::invalid_argument("Expected 'on' after 'create <index_type> index'.");
            }

            iss >> pq.table_name;

            if (!isValidIdentifier(pq.table_name)) {
                throw std::invalid_argument("Invalid table name: " + pq.table_name);
            }
            if (reservedKeywords.count(pq.table_name)) {
                throw std::invalid_argument("Reserved keyword used as table name: " + pq.table_name);
            }

            std::string by_word;
            iss >> by_word;
            std::transform(by_word.begin(), by_word.end(), by_word.begin(), ::tolower);
            if (by_word != "by") {
                throw std::invalid_argument("Expected 'by' after table name in 'create index'.");
            }

            std::string columns_str;
            std::getline(iss, columns_str);
            size_t first = columns_str.find_first_not_of(" \t\r\n");
            size_t last = columns_str.find_last_not_of(" \t\r\n");
            if (first != std::string::npos && last != std::string::npos) {
                columns_str = columns_str.substr(first, last - first + 1);
            }

            std::stringstream columns_stream(columns_str);
            std::string col;
            while (std::getline(columns_stream, col, ',')) {
                col.erase(col.begin(), std::find_if(col.begin(), col.end(),
                            [](unsigned char ch) { return !std::isspace(ch); }));
                col.erase(std::find_if(col.rbegin(), col.rend(),
                            [](unsigned char ch) { return !std::isspace(ch); }).base(), col.end());
                if (!isValidIdentifier(col)) {
                    throw std::invalid_argument("Invalid column name: " + col);
                }
                pq.index_columns.push_back(col);
            }
            if (db_ != nullptr) {
                auto table = db_->get_table(pq.table_name);
                for (const auto& col : pq.index_columns) {
                    if (!table->has_column(col)) {
                        throw std::invalid_argument("Column not found: " + col);
                    }
                }
            }
            
        }
        else {
            throw std::invalid_argument("Unknown CREATE subcommand: " + index_type_or_subcommand);
        }
    }
    else if (command == "insert") {
        pq.type = ParsedQuery::QueryType::Insert;
        pq.insert_values = std::vector<std::optional<Value>>();

        std::regex insert_regex(R"(^insert\s*\(([\s\S]*)\)\s*to\s+(\w+)$)", std::regex::icase);
        std::smatch matches;
        if (std::regex_match(trimmed_query, matches, insert_regex)) {
            std::string values_str = matches[1];
            std::string table_name = matches[2];

            if (!isValidIdentifier(table_name)) {
                throw std::invalid_argument("Invalid table name: " + table_name);
            }
            if (reservedKeywords.count(table_name)) {
                throw std::invalid_argument("Reserved keyword used as table name: " + table_name);
            }

            pq.table_name = table_name;

            if (!db_) {
                throw std::runtime_error("Database reference is not set in QueryParser.");
            }

            std::shared_ptr<memdb::core::Table> table;
            try {
                table = db_->get_table(table_name);
                const auto& columns = table->get_columns();
                pq.insert_values = parse_values(values_str, columns);
            } catch (exceptions::TableNotFoundException& e) {
                throw std::invalid_argument("Table does not exist: " + table_name);
            }
        }
        else {
            throw std::invalid_argument("Invalid INSERT syntax.");
        }
    }
    else if (command == "select") {
        pq.type = ParsedQuery::QueryType::Select;

        std::string rest_of_query;
        std::getline(iss, rest_of_query);
        rest_of_query = command + " " + rest_of_query;

        std::regex select_regex(R"(select\s+(.+?)\s+from\s+(\w+)(?:\s+join\s+(\w+)\s+on\s+(.+?))?(?:\s+where\s+(.+))?$)", std::regex::icase);        
        std::smatch matches;
        if (std::regex_match(rest_of_query, matches, select_regex)) {
            std::string columns_str = matches[1];
            std::string table_name = matches[2];

            if (!isValidIdentifier(table_name)) {
                throw std::invalid_argument("Invalid table name: " + table_name);
            }
            if (reservedKeywords.count(table_name)) {
                throw std::invalid_argument("Reserved keyword used as table name: " + table_name);
            }

            pq.table_name = table_name;

            std::vector<std::string> column_defs = split_columns(columns_str);

            std::regex select_col_regex(R"(^([^\s,]+(?:\s+[^\s,]+)*)(?:\s+as\s+(\w+))?$)", std::regex::icase);

            for (const auto& col_def : column_defs) {
                std::smatch col_matches;
                if (!std::regex_match(col_def, col_matches, select_col_regex)) {
                    throw std::invalid_argument("Invalid SELECT column definition: " + col_def);
                }

                std::string expr_str = col_matches[1];
                std::string alias = col_matches[2].matched ? col_matches[2].str() : "";

                expr_str = trim(expr_str);
                alias = trim(alias.empty() ? expr_str : alias); 

                ExpressionParser expr_parser(expr_str);
                auto expression = expr_parser.parse_expression();

                SelectItem select_col;
                select_col.expression = std::move(expression);
                select_col.alias = alias;

                pq.select_items.emplace_back(std::move(select_col));
            }

            if (matches[3].matched) {
                JoinInfo join_info;
                join_info.table_name = matches[3].str();
                std::string join_condition_str = matches[4].str();

                ExpressionParser expr_parser(join_condition_str);
                join_info.join_condition = expr_parser.parse_expression();

                pq.joins.push_back(std::move(join_info));
            }

            if (matches[5].matched) {
                std::string where_clause_str = matches[5].str();
                ExpressionParser expr_parser(where_clause_str);
                pq.where_clause = expr_parser.parse_expression();
            }
        } else {
            throw std::invalid_argument("Invalid SELECT syntax.");
        }
    }
    else if (command == "update") {
        pq.type = ParsedQuery::QueryType::Update;
        std::string table_name;
        iss >> table_name;
        pq.table_name = table_name;

        std::string set_word;
        iss >> set_word;
        std::transform(set_word.begin(), set_word.end(), set_word.begin(), ::tolower);
        if (set_word != "set") {
            throw std::invalid_argument("Expected \"set\" after table name in UPDATE.");
        }

        std::string rest;
        std::getline(iss, rest);

        size_t where_pos = rest.find("where");
        std::string assignments_str;
        std::string condition_str;

        if (where_pos != std::string::npos) {
            assignments_str = rest.substr(0, where_pos);
            condition_str = rest.substr(where_pos + 5);
        } else {
            assignments_str = rest;
        }

        auto trim = [](std::string& s) {
            size_t first = s.find_first_not_of(" \t\r\n");
            size_t last = s.find_last_not_of(" \t\r\n");
            if (first == std::string::npos || last == std::string::npos) {
                s = "";
            } else {
                s = s.substr(first, last - first + 1);
            }
        };

        trim(assignments_str);
        trim(condition_str);

        std::vector<std::string> assignments;
        int brace_level = 0;
        std::string current_assignment;
        for (char ch : assignments_str) {
            if (ch == '(' || ch == '{') {
                brace_level++;
            }
            if (ch == ')' || ch == '}') {
                brace_level--;
            }
            if (ch == ',' && brace_level == 0) {
                trim(current_assignment);
                if (!current_assignment.empty()) {
                    assignments.push_back(current_assignment);
                    current_assignment.clear();
                }
            } else {
                current_assignment += ch;
            }
        }

        trim(current_assignment);
        if (!current_assignment.empty()) {
            assignments.push_back(current_assignment);
        } else if (current_assignment.empty() && assignments.empty()) {
            throw std::invalid_argument("No assignment in UPDATE");
        }

        for (const auto& assign : assignments) {
            size_t eq_pos = assign.find('=');
            if (eq_pos == std::string::npos) {
                throw std::invalid_argument("Invalid assignment in UPDATE: " + assign);
            }
            std::string col_name = assign.substr(0, eq_pos);
            std::string expr_str = assign.substr(eq_pos + 1);

            trim(col_name);
            trim(expr_str);

            if (!isValidIdentifier(col_name)) {
                throw std::invalid_argument("Invalid column name: " + col_name);
            }
            if (reservedKeywords.count(col_name)) {
                throw std::invalid_argument("Reserved keyword used as column name: " + col_name);
            }

            ExpressionParser expr_parser(expr_str);
            auto expression = expr_parser.parse_expression();
            if (!expression) {
                throw std::invalid_argument("Failed to parse expression for column: " + col_name);
            }
            pq.update_assignments[col_name] = std::move(expression);
        }

        if (!condition_str.empty()) {
            ExpressionParser expr_parser(condition_str);
            auto condition_expression = expr_parser.parse_expression();
            if (!condition_expression) {
                throw std::invalid_argument("Failed to parse WHERE clause.");
            }
            pq.where_clause = std::move(condition_expression);
        }
    }
    else if (command == "delete") {
        pq.type = ParsedQuery::QueryType::Delete;
        std::string table_name;
        iss >> table_name;
        pq.table_name = table_name;

        std::string where_word;
        iss >> where_word;
        std::transform(where_word.begin(), where_word.end(), where_word.begin(), ::tolower);
        if (where_word != "where") {
            throw std::invalid_argument("Expected 'where' in DELETE.");
        }

        std::string condition_str;
        std::getline(iss, condition_str);
        size_t first = condition_str.find_first_not_of(" \t\r\n");
        size_t last = condition_str.find_last_not_of(" \t\r\n");
        if (first != std::string::npos && last != std::string::npos) {
            condition_str = condition_str.substr(first, last - first + 1);
        }
        ExpressionParser expr_parser(condition_str);
        pq.delete_where_clause = expr_parser.parse_expression();
    }
    else {
        throw std::invalid_argument("Unknown command: " + command);
    }

    return pq;
}

Column QueryParser::parse_column_definition(const std::string& col_def) {
    std::vector<ColumnAttribute> attributes;
    std::optional<Value> default_value = std::nullopt;
    std::string col_name;
    DataType data_type(Type::Int32);

    size_t start = 0;
    if (col_def[start] == '{') {
        size_t end = col_def.find('}', start);
        if (end == std::string::npos) {
            throw std::invalid_argument("Expected '}' for column attributes.");
        }
        std::string attrs_str = col_def.substr(start + 1, end - start - 1);
        std::stringstream attrs_stream(attrs_str);
        std::string attr;
        while (std::getline(attrs_stream, attr, ',')) {
            attr.erase(attr.begin(), std::find_if(attr.begin(), attr.end(),
                        [](unsigned char ch) { return !std::isspace(ch); }));
            attr.erase(std::find_if(attr.rbegin(), attr.rend(),
                        [](unsigned char ch) { return !std::isspace(ch); }).base(), attr.end());
            
            std::transform(attr.begin(), attr.end(), attr.begin(), ::tolower);

            if (attr == "unique") {
                attributes.emplace_back(ColumnAttribute::Unique);
            }
            else if (attr == "autoincrement") {
                attributes.emplace_back(ColumnAttribute::AutoIncrement);
            }
            else if (attr == "key") {
                attributes.emplace_back(ColumnAttribute::Key);
            }
            else {
                throw std::invalid_argument("Unknown column attribute: " + attr);
            }
        }
        start = end + 1;
    }

    size_t colon_pos = col_def.find(':', start);
    if (colon_pos == std::string::npos) {
        throw std::invalid_argument("Expected ':' in column definition.");
    }

    std::string name_part = col_def.substr(start, colon_pos - start);
    name_part.erase(name_part.begin(), std::find_if(name_part.begin(), name_part.end(),
                    [](unsigned char ch) { return !std::isspace(ch); }));
    name_part.erase(std::find_if(name_part.rbegin(), name_part.rend(),
                    [](unsigned char ch) { return !std::isspace(ch); }).base(), name_part.end());

    if (name_part.empty()) {
        throw std::invalid_argument("Column name is empty.");
    }
    col_name = name_part;

    size_t eq_pos = col_def.find('=', colon_pos + 1);
    std::string type_str;
    if (eq_pos != std::string::npos) {
        type_str = col_def.substr(colon_pos + 1, eq_pos - colon_pos - 1);
        type_str = trim(type_str);

        Type type;
        size_t size = 0;
        if (type_str.find("string[") != std::string::npos || type_str.find("bytes[") != std::string::npos) {
            size = std::stoul(type_str.substr(type_str.find('[') + 1,
                                        type_str.find(']') - type_str.find('[') - 1));
        }

        std::string default_val_str = col_def.substr(eq_pos + 1);
        default_val_str = trim(default_val_str);

        Lexer lexer(default_val_str);
        Token token = lexer.get_next_token();

        if (token.type == TokenType::EndOfInput) {
            throw std::invalid_argument("Invalid default value: " + default_val_str);
        }

        default_value = parse_value(token.type, token.value, size);
    } else {
        type_str = col_def.substr(colon_pos + 1);
        type_str = trim(type_str);
    }

    std::transform(type_str.begin(), type_str.end(), type_str.begin(), ::tolower);

    Type type;
    size_t size = 0;
    if (type_str.find("int32") != std::string::npos) {
        type = Type::Int32;
    }
    else if (type_str.find("string") != std::string::npos) {
        type = Type::String;
        size = std::stoul(type_str.substr(type_str.find('[') + 1,
                                           type_str.find(']') - type_str.find('[') - 1));
    }
    else if (type_str.find("bytes") != std::string::npos) {
        type = Type::Bytes;
        size = std::stoul(type_str.substr(type_str.find('[') + 1,
                                           type_str.find(']') - type_str.find('[') - 1));
    }
    else if (type_str.find("bool") != std::string::npos) {
        type = Type::Bool;
    }
    else {
        throw std::invalid_argument("Unknown column type: " + type_str);
    }

    if (type == Type::String || type == Type::Bytes) {
        data_type = DataType(type, size);
    }
    else {
        data_type = DataType(type);
    }

    return Column(col_name, data_type, attributes, default_value);
}

std::vector<std::string> QueryParser::split_columns(const std::string& columns_def) {
    std::vector<std::string> columns;
    int brace_level = 0;
    int paren_level = 0;
    std::string current;
    
    for (char ch : columns_def) {
        if (ch == '{') {
            brace_level++;
            current += ch;
        }
        else if (ch == '}') {
            if (brace_level == 0) {
                throw std::invalid_argument("Unbalanced braces in column definitions.");
            }
            brace_level--;
            current += ch;
        }
        else if (ch == '(') {
            paren_level++;
            current += ch;
        }
        else if (ch == ')') {
            if (paren_level == 0) {
                throw std::invalid_argument("Unbalanced parantheses in column definitions.");
            }
            paren_level--;
            current += ch;
        }
        else if (ch == ',' && brace_level == 0 && paren_level == 0) {
            size_t first = current.find_first_not_of(" \t\r\n");
            size_t last = current.find_last_not_of(" \t\r\n");
            if (first != std::string::npos && last != std::string::npos) {
                columns.emplace_back(current.substr(first, last - first + 1));
            }
            current.clear();
        }
        else {
            current += ch;
        }
    }
    
    size_t first = current.find_first_not_of(" \t\r\n");
    size_t last = current.find_last_not_of(" \t\r\n");
    if (first != std::string::npos && last != std::string::npos) {
        columns.emplace_back(current.substr(first, last - first + 1));
    }

    if (brace_level != 0 || paren_level != 0) {
        throw std::invalid_argument("Unbalanced parentheses or braces in column definitions.");
    }
    
    return columns;
}

std::string QueryParser::trim(const std::string& s) {
    auto start = std::find_if_not(s.begin(), s.end(), [](unsigned char ch) {
        return std::isspace(ch);
    });
    auto end = std::find_if_not(s.rbegin(), s.rend(), [](unsigned char ch) {
        return std::isspace(ch);
    }).base();
    
    return (start < end ? std::string(start, end) : "");
}

}
}