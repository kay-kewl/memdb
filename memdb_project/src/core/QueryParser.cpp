#include "memdb/core/QueryParser.h"
#include "memdb/core/ExpressionParser.h"

#include <sstream>
#include <algorithm>
#include <cctype>
#include <limits>
#include <stdexcept>
#include <regex>

namespace memdb {
namespace core {

Value parse_value(TokenType token_type, const std::string& value_str) {
    if (value_str.empty()) {
        throw std::invalid_argument("Empty value.");
    }

    switch (token_type) {
        case TokenType::StringLiteral:
            // The value_str is already the string without quotes
            return Value(value_str);

        case TokenType::BytesLiteral: {
            std::string hex_str = value_str.substr(2);
            if (hex_str.size() % 2 != 0) {
                throw std::invalid_argument("Invalid hex string for bytes.");
            }
            std::vector<uint8_t> bytes;
            bytes.reserve(hex_str.size() / 2);
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

std::vector<std::optional<Value>> parse_values(const std::string& values_str) {
    std::vector<std::optional<Value>> values;
    Lexer lexer(values_str);
    Token token = lexer.get_next_token();
    while (token.type != TokenType::EndOfInput) {
        if (token.type == TokenType::Comma) {
            values.push_back(std::nullopt);
            token = lexer.get_next_token();
            continue;
        }

        // Parse the value
        std::string value_str;
        if (token.type == TokenType::StringLiteral || token.type == TokenType::BytesLiteral ||
            token.type == TokenType::IntLiteral || token.type == TokenType::BoolLiteral) {
            Value value = parse_value(token.type, token.value);
            values.push_back(value);
        } else {
            throw std::invalid_argument("Invalid value in INSERT statement: " + token.value);
        }

        // Expect either a comma or end of input
        token = lexer.get_next_token();
        if (token.type == TokenType::Comma) {
            token = lexer.get_next_token();
        }
    }
    return values;
}

ParsedQuery QueryParser::parse(const std::string& query) {
    std::string trimmed_query = trim(query);

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

            if (pq.table_name.empty()) {
                throw std::invalid_argument("Table name cannot be empty.");
            }

            char c;
            iss >> c;
            if (c != '(') {
                throw std::invalid_argument("Expected '(' after table name.");
            }

            std::string columns_def;
            std::getline(iss, columns_def, ')');

            std::vector<std::string> column_defs = split_columns(columns_def);

            for (const auto& col_def : column_defs) {
                Column column = parse_column_definition(col_def);
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
                pq.index_columns.push_back(col);
            }
        }
        else {
            throw std::invalid_argument("Unknown CREATE subcommand: " + index_type_or_subcommand);
        }
    }
    else if (command == "insert") {
        // insert (,"vasya", Oxdeadbeefdeadbeef) to users
        // insert (login = "vasya", password_hash = Oxdeadbeefdeadbeef) to users
        pq.type = ParsedQuery::QueryType::Insert;
        pq.insert_values = std::vector<std::optional<Value>>();

        std::regex insert_regex(R"(^insert\s*\((.*)\)\s*to\s+(\w+)$)", std::regex::icase);
        std::smatch matches;
        if (std::regex_match(trimmed_query, matches, insert_regex)) {
            std::string values_str = matches[1];
            std::string table_name = matches[2];
            pq.table_name = table_name;

            pq.insert_values = parse_values(values_str);
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

            pq.table_name = table_name;

            // Parse the column expressions to select
        //     std::stringstream columns_stream(columns_str);
        //     std::string col;
        //     while (std::getline(columns_stream, col, ',')) {
        //         // Trim whitespace
        //         col.erase(col.begin(), std::find_if(col.begin(), col.end(),
        //                     [](unsigned char ch) { return !std::isspace(ch); }));
        //         col.erase(std::find_if(col.rbegin(), col.rend(),
        //                     [](unsigned char ch) { return !std::isspace(ch); }).base(), col.end());
    
        //         // Now, parse expressions and optional aliases
        //         // Check if there is an 'as' keyword
        //         std::regex alias_regex(R"((.+?)\s+(?:as\s+)?(\w+)$)", std::regex::icase);
        //         std::smatch alias_match;
        //         std::unique_ptr<Expression> expr;
        //         std::string alias;
        //         if (std::regex_match(col, alias_match, alias_regex)) {
        //             std::string expr_str = alias_match[1];
        //             alias = alias_match[2];
                    
        //             // Parse the expression
        //             ExpressionParser expr_parser(expr_str);
        //             expr = expr_parser.parse_expression();
        //         } else {
        //             // No alias
        //             // Parse the expression
        //             ExpressionParser expr_parser(col);
        //             expr = expr_parser.parse_expression();
                    
        //             // Optionally, set alias as the expression string (or empty)
        //             alias = ""; // or you can generate a default alias
        //         }
                
        //         ParsedQuery::SelectItem select_item{std::move(expr), alias};
        //         pq.select_items.push_back(std::move(select_item));
        //     }

        //     // Handle JOIN if present
        //     if (matches[3].matched) {
        //         ParsedQuery::JoinInfo join_info;
        //         join_info.table_name = matches[3].str();
        //         std::string join_condition_str = matches[4].str();

        //         ExpressionParser expr_parser(join_condition_str);
        //         join_info.join_condition = expr_parser.parse_expression();

        //         pq.joins.push_back(std::move(join_info));
        //     }

        //     // Handle WHERE clause if present
        //     if (matches[5].matched) {
        //         std::string where_clause_str = matches[5].str();
        //         ExpressionParser expr_parser(where_clause_str);
        //         pq.where_clause = expr_parser.parse_expression();
        //     }
        // } else {
        //     throw std::invalid_argument("Invalid SELECT syntax.");
        // }
            std::vector<std::string> column_defs = split_columns(columns_str);

            // std::regex select_col_regex(R"(^(.+?)(?:\s+as\s+(\w+))?$)", std::regex::icase);
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

                ParsedQuery::SelectItem select_col;
                select_col.expression = std::move(expression);
                select_col.alias = alias;

                pq.select_items.emplace_back(std::move(select_col));
            }

            if (matches[3].matched) {
                ParsedQuery::JoinInfo join_info;
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
            throw std::invalid_argument("Expected 'set' after table name in UPDATE.");
        }

        std::string assignments_str;
        std::getline(iss, assignments_str, 'w'); 

        std::string where_part;
        std::getline(iss, where_part);
        size_t where_pos = assignments_str.find("where");
        if (where_pos != std::string::npos) {
            assignments_str = assignments_str.substr(0, where_pos);
        }

        std::stringstream assign_stream(assignments_str);
        std::string assign;
        while (std::getline(assign_stream, assign, ',')) {
            size_t eq_pos = assign.find('=');
            if (eq_pos == std::string::npos) {
                throw std::invalid_argument("Invalid assignment in UPDATE.");
            }
            std::string col_name = assign.substr(0, eq_pos);
            std::string expr_str = assign.substr(eq_pos + 1);

            size_t first = col_name.find_first_not_of(" \t\r\n");
            size_t last = col_name.find_last_not_of(" \t\r\n");
            if (first != std::string::npos && last != std::string::npos) {
                col_name = col_name.substr(first, last - first + 1);
            }

            first = expr_str.find_first_not_of(" \t\r\n");
            last = expr_str.find_last_not_of(" \t\r\n");
            if (first != std::string::npos && last != std::string::npos) {
                expr_str = expr_str.substr(first, last - first + 1);
            }

            ExpressionParser expr_parser(expr_str);
            pq.update_assignments[col_name] = expr_parser.parse_expression();
        }

        if (where_pos != std::string::npos) {
            std::string condition_str = assignments_str.substr(where_pos + 5); // 5 symbols 'where'
            size_t first = condition_str.find_first_not_of(" \t\r\n");
            size_t last = condition_str.find_last_not_of(" \t\r\n");
            if (first != std::string::npos && last != std::string::npos) {
                condition_str = condition_str.substr(first, last - first + 1);
            }
            ExpressionParser expr_parser(condition_str);
            pq.update_where_clause = expr_parser.parse_expression();
        }
    }
    else if (command == "delete") {
        pq.type = ParsedQuery::QueryType::Delete;
        std::string from_word;
        iss >> from_word;
        std::transform(from_word.begin(), from_word.end(), from_word.begin(), ::tolower);
        if (from_word != "from") {
            throw std::invalid_argument("Expected 'from' after 'delete'.");
        }

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

        std::string default_val_str = col_def.substr(eq_pos + 1);
        default_val_str = trim(default_val_str);

        // Tokenize the default value string
        Lexer lexer(default_val_str);
        Token token = lexer.get_next_token();

        if (token.type == TokenType::EndOfInput) {
            throw std::invalid_argument("Invalid default value: " + default_val_str);
        }

        // Parse the default value using the token's type and value
        default_value = parse_value(token.type, token.value);
    } else {
        type_str = col_def.substr(colon_pos + 1);
        type_str = trim(type_str);
    }

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

    bool flag = false;

    for (const auto& attr : attributes) {
        switch (attr) {
            case ColumnAttribute::Unique:
                flag = true;
                std::cout << "unique ";
                break;
            case ColumnAttribute::AutoIncrement:
                flag = true;
                std::cout << "autoincrement ";
                break;
            case ColumnAttribute::Key:
                flag = true;
                std::cout << "key ";
                break;
            default:
                std::cout << "unknown ";
        }
    }
    if (default_value) {
        std::cout << ", Default: " << default_value->to_string();
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

} // namespace core
} // namespace memdb