#include "memdb/core/Row.h"
#include "memdb/core/Table.h"
#include "memdb/core/QueryParser.h"

#include "memdb/core/exceptions/DatabaseException.h"

namespace memdb {
namespace core {

using json = nlohmann::json;

Table::Table(const std::string& name, const std::vector<Column>& columns) : name_(name), columns_(columns), next_row_id_(1) {
    if (name_.empty()) {
        throw std::invalid_argument("Table name cannot be empty");
    }

    if (columns_.empty()) {
        throw std::invalid_argument("Column definitions cannot be empty");
    }

    std::unordered_map<std::string, bool> column_names;
    for (const auto& column : columns_) {
        if (column_names.find(column.get_name()) != column_names.end()) {
            throw std::invalid_argument("Duplicate column name: " + column.get_name());
        }
        column_names[column.get_name()] = true;
    }
}

const std::string& Table::get_name() const {
    return name_;
}

const std::vector<Column>& Table::get_columns() const {
    return columns_;
}

size_t Table::get_column_index(const std::string& column_name) const {
    for (size_t i = 0; i < columns_.size(); ++i) {
        if (columns_[i].get_name() == column_name) {
            return i;
        }
    }
    throw std::invalid_argument("Column not found: " + column_name);
}

void Table::add_column(const Column& column) {
    if (has_column(column.get_name())) {
        throw std::invalid_argument("Column already exists: " + column.get_name());
    }

    columns_.push_back(column);

    for (auto& [id, row] : rows_) {
        if (column.get_default_value()) {
            row.get_values().emplace_back(*(column.get_default_value()));
        }
        else if (column.get_type().is_string()) {
            row.get_values().emplace_back(std::string());
        }
        else if (column.get_type().is_bytes()) {
            row.get_values().emplace_back(std::vector<uint8_t>());
        }
        else if (column.get_type().is_int32()) {
            row.get_values().emplace_back(int32_t(0));
        }
        else if (column.get_type().is_bool()) {
            row.get_values().emplace_back(bool(false));
        }
        else {
            row.get_values().emplace_back(std::nullopt);
        }
    }
}

bool Table::has_column(const std::string& column_name) const {
    for (const auto& column : columns_) {
        if (column.get_name() == column_name) {
            return true;
        }
    }
    return false;
}

RowID Table::insert_row(const std::vector<std::optional<Value>>& values) {
    validate_row(values);

    std::vector<std::optional<Value>> complete_values;
    complete_values.reserve(columns_.size());

    for (size_t i = 0; i < columns_.size(); ++i) {
        if (i < values.size() && values[i].has_value()) {
            complete_values.emplace_back(values[i]);
        }
        else {
            const auto& column = columns_[i];
            if (column.has_attribute(ColumnAttribute::AutoIncrement)) {
                complete_values.emplace_back(static_cast<int32_t>(next_row_id_));
            }
            else if (column.get_default_value()) {
                complete_values.emplace_back(*(column.get_default_value()));
            }
            else {
                complete_values.emplace_back(std::nullopt);
            }
        }
    }

    RowID new_id = next_row_id_++;
    rows_.emplace(new_id, Row(new_id, complete_values));
    return new_id;
}

RowID Table::insert_row(const std::vector<std::optional<Value>>& values, RowID id) {
    validate_row(values);

    std::vector<std::optional<Value>> complete_values = values;

    for (size_t i = 0; i < columns_.size(); ++i) {
        const auto& column = columns_[i];

        if (column.has_attribute(ColumnAttribute::AutoIncrement) && !complete_values[i].has_value()) {
            complete_values[i] = static_cast<int32_t>(next_row_id_);
        }
        else if (!complete_values[i].has_value() && column.get_default_value()) {
            complete_values[i] = *(column.get_default_value());
        }
    }

    RowID new_id;
    if (id != 0) {
        new_id = id;
        if (new_id >= next_row_id_) {
            next_row_id_ = new_id + 1;
        }
    }
    else {
        new_id = next_row_id_++;
    }

    for (size_t i = 0; i < columns_.size(); ++i) {
        const auto& column = columns_[i];
        if (column.has_attribute(ColumnAttribute::Unique) || column.has_attribute(ColumnAttribute::Key)) {
            if (complete_values[i].has_value()) {
                const Value& new_val = *(complete_values[i]);

                for (const auto& [existing_id, existing_row] : rows_) {
                    if (existing_id == new_id) {
                        continue;
                    }

                    const auto& existing_val_opt = existing_row.get_value(i);
                    if (existing_val_opt.has_value()) {
                        const Value& existing_val = *(existing_val_opt);

                        if (new_val.get_type() != existing_val.get_type()) {
                            continue;
                        }

                        bool equal = false;
                        switch (new_val.get_type()) {
                            case Type::Int32:
                                equal = (new_val.get_int() == existing_val.get_int());
                                break;
                            case Type::Bool:
                                equal = (new_val.get_bool() == existing_val.get_bool());
                                break;
                            case Type::String:
                                equal = (new_val.get_string() == existing_val.get_string());
                                break;
                            case Type::Bytes:
                                equal = (new_val.get_bytes() == existing_val.get_bytes());
                                break;
                            default:
                                break;
                        }

                        if (equal) {
                            throw std::invalid_argument("Duplicate value for unique/key column '" + column.get_name() + "'.");
                        }
                    }
                }
            }
        }
    }

    rows_.emplace(new_id, Row(new_id, complete_values));
    return new_id;
}

void Table::delete_row(RowID id) {
    auto it = rows_.find(id);
    if (it != rows_.end()) {
        rows_.erase(it);
    }
    else {
        throw std::invalid_argument("Row ID not found: " + std::to_string(id));
    }
}

Row& Table::get_row(RowID id) {
    auto it = rows_.find(id);
    if (it != rows_.end()) {
        return it->second;
    }
    throw std::invalid_argument("Row ID not found: " + std::to_string(id));
}

const Row& Table::get_row(RowID id) const {
    auto it = rows_.find(id);
    if (it != rows_.end()) {
        return it->second;
    }
    throw std::invalid_argument("Row ID not found: " + std::to_string(id));
}

std::map<RowID, Row>& Table::get_all_rows() {
    return rows_;
}

const std::map<RowID, Row>& Table::get_all_rows() const {
    return rows_;
}


void Table::add_index(const std::string& index_type_str, const std::vector<std::string>& columns) {
    IndexType index_type;
    if (index_type_str == "ordered") {
        index_type = IndexType::Ordered;
    }
    else if (index_type_str == "unordered") {
        index_type = IndexType::Unordered;
    }
    else {
        throw std::invalid_argument("Unknown index type: " + index_type_str);
    }

    std::unique_ptr<Index> index = std::make_unique<Index>(index_type, columns);

    for (const auto& [row_id, row] : rows_) {
        std::unordered_map<std::string, Value> row_map;
        for (const auto& col_name : columns) {
            size_t col_index = get_column_index(col_name);
            if (row.get_values()[col_index].has_value()) {
                row_map[col_name] = *(row.get_values()[col_index]);
            }
            else {
                throw std::invalid_argument("Cannot index NULL value in column '" + col_name + "'.");
            }
        }
        index->add_row(row_id, row_map);
    }

    indexes_.emplace_back(std::move(index));
}

std::vector<RowID> Table::find_rows(const std::unique_ptr<Expression>& condition) const {
    std::vector<RowID> matching_rows;
    for (const auto& [row_id, row] : rows_) {
        std::unordered_map<std::string, Value> row_map;
        for (size_t i = 0; i < columns_.size(); ++i) {
            if (row.get_values()[i].has_value()) {
                row_map[columns_[i].get_name()] = *(row.get_values()[i]);
            }
        }

        bool match = true;
        if (condition) {
            Value cond = condition->evaluate(row_map);
            if (cond.get_type() != Type::Bool) {
                throw std::invalid_argument("WHERE clause does not evaluate to a boolean.");
            }
            match = cond.get_bool();
        }

        if (match) {
            matching_rows.push_back(row_id);
        }
    }

    return matching_rows;
}

void Table::validate_row(const std::vector<std::optional<Value>>& values) const {
    if (values.size() > columns_.size()) {
        throw std::invalid_argument("Too many values provided for insertion.");
    }

    for (size_t i = 0; i < values.size(); ++i) {
        if (!values[i].has_value()) {
            continue;
        }

        const auto& value = *(values[i]);
        const auto& column = columns_[i];

        if (value.get_type() != column.get_type().get_type()) {
            throw std::invalid_argument("Type mismatch for column \"" + column.get_name() +
                                        "\". Expected: " + column.get_type().to_string() +
                                        ", Got: " + value.to_string());
        }

        if (column.get_type().is_string()) {
            if (value.get_string().size() > column.get_type().get_size()) {
                throw std::invalid_argument("Value for column \"" + column.get_name() +
                                            "\" exceeds maximum length.");
            }
        }
        else if (column.get_type().is_bytes()) {
            if (value.get_bytes().size() > column.get_type().get_size()) {
                throw std::invalid_argument("Value for column \"" + column.get_name() +
                                            "\" exceeds maximum byte size.");
            }
        }
    }

    for (size_t i = 0; i < values.size(); ++i) {
        if (!values[i].has_value()) {
            continue;
        }

        const auto& column = columns_[i];
        if (column.has_attribute(ColumnAttribute::Unique) || column.has_attribute(ColumnAttribute::Key)) {
            const Value& new_value = *(values[i]);

            for (const auto& [existing_id, existing_row] : rows_) {
                const auto& existing_value_opt = existing_row.get_value(i);
                if (existing_value_opt.has_value()) {
                    const Value& existing_value = *(existing_value_opt);
                    if (new_value.get_type() == existing_value.get_type()) {
                        bool equal = false;
                        switch (new_value.get_type()) {
                            case Type::Int32:
                                equal = (new_value.get_int() == existing_value.get_int());
                                break;
                            case Type::Bool:
                                equal = (new_value.get_bool() == existing_value.get_bool());
                                break;
                            case Type::String:
                                equal = (new_value.get_string() == existing_value.get_string());
                                break;
                            case Type::Bytes:
                                equal = (new_value.get_bytes() == existing_value.get_bytes());
                                break;
                            default:
                                break;
                        }
                        if (equal) {
                            throw std::invalid_argument("Duplicate value for unique/key column \"" +
                                                        column.get_name() + "\".");
                        }
                    }
                }
            }
        }
    }
}

void Table::validate_row_update(const std::vector<std::optional<Value>>& updated_values, RowID current_row_id) const {
    if (updated_values.size() > columns_.size()) {
        throw std::invalid_argument("Too many values provided for the row update.");
    }

    for (size_t i = 0; i < updated_values.size(); ++i) {
        if (!updated_values[i].has_value()) {
            continue; 
        }

        const Value& value = updated_values[i].value();
        const Column& column = columns_[i];

        if (value.get_type() != column.get_type().get_type()) {
            throw std::invalid_argument("Type mismatch for column '" + column.get_name() +
                                        "'. Expected: " + column.get_type().to_string() +
                                        ", Got: " + value.to_string());
        }

        if (column.get_type().is_string()) {
            if (value.get_string().size() > column.get_type().get_size()) {
                throw std::invalid_argument("Value for column \"" + column.get_name() +
                                            "\" exceeds maximum length of " + std::to_string(column.get_type().get_size()) + ".");
            }
        }
        else if (column.get_type().is_bytes()) {
            if (value.get_bytes().size() > column.get_type().get_size()) {
                throw std::invalid_argument("Value for column \"" + column.get_name() +
                                            "\" exceeds maximum byte size of " + std::to_string(column.get_type().get_size()) + ".");
            }
        }
    }

    for (size_t i = 0; i < updated_values.size(); ++i) {
        if (!updated_values[i].has_value()) {
            continue;
        }

        const Column& column = columns_[i];
        if (column.has_attribute(ColumnAttribute::Unique) || column.has_attribute(ColumnAttribute::Key)) {
            const Value& new_value = updated_values[i].value();

            for (const auto& [existing_id, existing_row] : rows_) {
                if (existing_id == current_row_id) {
                    continue; 
                }

                const auto& existing_value_opt = existing_row.get_value(i);
                if (!existing_value_opt.has_value()) {
                    continue;
                }

                const Value& existing_value = existing_value_opt.value();

                bool equal = false;
                switch (new_value.get_type()) {
                    case Type::Int32:
                        equal = (new_value.get_int() == existing_value.get_int());
                        break;
                    case Type::Bool:
                        equal = (new_value.get_bool() == existing_value.get_bool());
                        break;
                    case Type::String:
                        equal = (new_value.get_string() == existing_value.get_string());
                        break;
                    case Type::Bytes:
                        equal = (new_value.get_bytes() == existing_value.get_bytes());
                        break;
                    default:
                        break;
                }

                if (equal) {
                    throw std::invalid_argument("Duplicate value for unique/key column '" +
                                                column.get_name() + "'.");
                }
            }
        }
    }
}

nlohmann::json Table::to_json() const {
    nlohmann::json j;
    j["name"] = name_;

    j["columns"] = nlohmann::json::array();
    for (const auto& column : columns_) {
        nlohmann::json col_json;
        col_json["name"] = column.get_name();
        col_json["type"] = column.get_type().to_string();

        nlohmann::json attrs = nlohmann::json::array();
        for (const auto& attr : column.get_attributes()) {
            switch (attr) {
                case ColumnAttribute::Unique:
                    attrs.push_back("unique");
                    break;
                case ColumnAttribute::AutoIncrement:
                    attrs.push_back("autoincrement");
                    break;
                case ColumnAttribute::Key:
                    attrs.push_back("key");
                    break;
                default:
                    attrs.push_back("unknown");
            }
        }
        col_json["attributes"] = attrs;

        if (column.get_default_value()) {
            col_json["default"] = column.get_default_value()->to_string();
        }

        j["columns"].push_back(col_json);
    }

    j["rows"] = nlohmann::json::array();
    for (const auto& [id, row] : rows_) {
        nlohmann::json row_json;
        row_json["id"] = id;
        row_json["values"] = nlohmann::json::array();
        for (const auto& value : row.get_values()) {
            if (value.has_value()) {
                switch (value->get_type()) {
                    case Type::Int32:
                        row_json["values"].push_back(value->get_int());
                        break;
                    case Type::Bool:
                        row_json["values"].push_back(value->get_bool());
                        break;
                    case Type::String:
                        row_json["values"].push_back(value->get_string());
                        break;
                    case Type::Bytes:
                        {
                            const auto& bytes = value->get_bytes();
                            std::string hex_str = "0x";
                            const char hex_chars[] = "0123456789ABCDEF";
                            for (uint8_t byte : bytes) {
                                hex_str += hex_chars[(byte >> 4) & 0xF];
                                hex_str += hex_chars[byte & 0xF];
                            }
                            row_json["values"].push_back(hex_str);
                        }
                        break;
                    default:
                        row_json["values"].push_back(nullptr);
                        break;
                }
            }
            else {
                row_json["values"].push_back(nullptr);
            }
        }
        j["rows"].push_back(row_json);
    }

    return j;
}

Table Table::from_json(const nlohmann::json& j) {
    std::string table_name = j.at("name").get<std::string>();
    std::vector<Column> columns;

    for (const auto& col_json : j.at("columns")) {
        std::string col_name = col_json.at("name").get<std::string>();
        std::string col_type_str = col_json.at("type").get<std::string>();

        Type type;
        size_t size = 0;
        if (col_type_str.find("int32") != std::string::npos) {
            type = Type::Int32;
        }
        else if (col_type_str.find("bool") != std::string::npos) {
            type = Type::Bool;
        }
        else if (col_type_str.find("string") != std::string::npos) {
            type = Type::String;
            size = std::stoul(col_type_str.substr(col_type_str.find('[') + 1,
                        col_type_str.find_last_of(']') - col_type_str.find('[') - 1));
        }
        else if (col_type_str.find("bytes") != std::string::npos) {
            type = Type::Bytes;
            size = std::stoul(col_type_str.substr(col_type_str.find('[') + 1,
                        col_type_str.find_last_of(']') - col_type_str.find('[') - 1));
        }
        else {
            throw exceptions::SerializationException("Unknown column type: " + col_type_str);
        }

        std::vector<ColumnAttribute> attributes;
        for (const auto& attr : col_json.at("attributes")) {
            std::string attr_str = attr.get<std::string>();
            if (attr_str == "unique") {
                attributes.emplace_back(ColumnAttribute::Unique);
            }
            else if (attr_str == "autoincrement") {
                attributes.emplace_back(ColumnAttribute::AutoIncrement);
            }
            else if (attr_str == "key") {
                attributes.emplace_back(ColumnAttribute::Key);
            }
            else {
                // TODO: unknown attributes
            }
        }

        std::optional<Value> default_value = std::nullopt;
        if (col_json.contains("default")) {
            std::string default_val_str = col_json.at("default").get<std::string>();
            switch (type) {
                case Type::Int32:
                    default_value = Value(static_cast<int32_t>(std::stoi(default_val_str)));
                    break;
                case Type::Bool:
                    default_value = Value((default_val_str == "true"));
                    break;
                case Type::String:
                    if (default_val_str.front() == '"' && default_val_str.back() == '"') {
                        default_val_str = default_val_str.substr(1, default_val_str.size() - 2);
                    }
                    default_value = Value(default_val_str);
                    break;
                case Type::Bytes:
                {
                    std::string hex_str = default_val_str;
                    if (hex_str.find("0x") != 0 && hex_str.find("0X") != 0) {
                        throw exceptions::SerializationException("Invalid bytes format.");
                    }
                    hex_str = hex_str.substr(2);
                    if (hex_str.size() % 2 != 0) {
                        throw exceptions::SerializationException("Invalid hex length for bytes.");
                    }
                    std::vector<uint8_t> bytes;
                    bytes.reserve(hex_str.size() / 2);
                    for (size_t i = 0; i < hex_str.size(); i += 2) {
                        uint8_t byte = static_cast<uint8_t>(std::stoi(hex_str.substr(i, 2), nullptr, 16));
                        bytes.push_back(byte);
                    }
                    default_value = Value(bytes);
                }
                break;
                default:
                    throw exceptions::SerializationException("Unhandled column type: " + col_type_str);
            }
        }

        DataType data_type = (type == Type::String || type == Type::Bytes)
                               ? DataType(type, size)
                               : DataType(type);

        columns.emplace_back(col_name, data_type, attributes, default_value);
    }

    Table table(table_name, columns);

    for (const auto& row_json : j.at("rows")) {
        core::RowID row_id = row_json.at("id").get<core::RowID>();
        std::vector<std::optional<Value>> values;
        const auto& cols = table.get_columns();
        const auto& val_array = row_json.at("values");

        for (size_t i = 0; i < cols.size(); ++i) {
            const auto& col = cols[i];
            const auto& val_json = val_array[i];

            if (val_json.is_null()) {
                values.emplace_back(std::nullopt);
            }
            else {
                switch (col.get_type().get_type()) {
                    case Type::Int32:
                        values.emplace_back(Value(static_cast<int32_t>(val_json.get<int32_t>())));
                        break;
                    case Type::Bool:
                        values.emplace_back(Value(val_json.get<bool>()));
                        break;
                    case Type::String:
                        values.emplace_back(Value(val_json.get<std::string>()));
                        break;
                    case Type::Bytes:
                        {
                            std::string hex_str = val_json.get<std::string>();
                            if (hex_str.find("0x") != 0 && hex_str.find("0X") != 0) {
                                throw exceptions::SerializationException("Invalid bytes format.");
                            }
                            hex_str = hex_str.substr(2);
                            if (hex_str.size() % 2 != 0) {
                                throw exceptions::SerializationException("Invalid hex length for bytes.");
                            }
                            std::vector<uint8_t> bytes;
                            bytes.reserve(hex_str.size() / 2);
                            for (size_t j = 0; j < hex_str.size(); j += 2) {
                                uint8_t byte = static_cast<uint8_t>(std::stoi(hex_str.substr(j, 2), nullptr, 16));
                                bytes.push_back(byte);
                            }
                            values.emplace_back(Value(bytes));
                        }
                        break;
                    default:
                        throw exceptions::SerializationException("Unknown column type: " + col.get_type().to_string());
                }
            }
        }

        table.insert_row(values, row_id);
    }

    return table;
}

std::string Table::to_string() const {
    std::ostringstream oss;
    oss << "Table: " << name_ << "\nColumns:\n";
    for (const auto& column : columns_) {
        oss << "  " << column.to_string() << "\n";
    }
    oss << "Rows:\n";
    for (const auto& [id, row] : rows_) {
        oss << "  RowID " << id << ": ";
        for (size_t i = 0; i < row.get_values().size(); ++i) {
            if (i > 0) oss << ", ";
            if (row.get_values()[i].has_value()) {
                oss << row.get_values()[i]->to_string();
            } else {
                oss << "NULL";
            }
        }
        oss << "\n";
    }
    return oss.str();
}

} 
} 