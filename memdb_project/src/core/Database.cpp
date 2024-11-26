#include "memdb/core/Database.h"

#include "memdb/core/exceptions/DatabaseException.h"

#include <fstream>
#include <json.hpp>

namespace memdb {
namespace core {

using json = nlohmann::json;

// ======= Database Implementation =======

Database::Database() = default;

void Database::create_table(const std::string& table_name, const std::vector<Column>& columns) {
    if (has_table(table_name)) {
        throw exceptions::TableAlreadyExistsException(table_name);
    }
    auto table = std::make_shared<Table>(table_name, columns);
    tables_.emplace(table_name, table);
}

void Database::drop_table(const std::string& table_name) {
    auto it = tables_.find(table_name);
    if (it != tables_.end()) {
        tables_.erase(it);
    } else {
        throw exceptions::TableNotFoundException(table_name);
    }
}

std::shared_ptr<Table> Database::get_table(const std::string& table_name) const {
    auto it = tables_.find(table_name);
    if (it != tables_.end()) {
        return it->second;
    }
    throw exceptions::TableNotFoundException(table_name);
}

bool Database::has_table(const std::string& table_name) const {
    return tables_.find(table_name) != tables_.end();
}

core::RowID Database::insert_row(const std::string& table_name, const std::vector<std::optional<Value>>& values) {
    auto table = get_table(table_name);
    return table->insert_row(values);
}

void Database::delete_row(const std::string& table_name, core::RowID row_id) {
    auto table = get_table(table_name);
    table->delete_row(row_id);
}

Row& Database::get_row(const std::string& table_name, core::RowID row_id) {
    auto table = get_table(table_name);
    return table->get_row(row_id);
}

const Row& Database::get_row(const std::string& table_name, core::RowID row_id) const {
    auto table = get_table(table_name);
    return table->get_row(row_id);
}

const std::unordered_map<std::string, std::shared_ptr<Table>>& Database::get_all_tables() const {
    return tables_;
}

void Database::save_to_file(const std::string& filename) const {
    json j;
    j["tables"] = json::array();
    for (const auto& [name, table] : tables_) {
        j["tables"].push_back(table->to_json());
    }
    std::ofstream ofs(filename, std::ios::binary);
    if (!ofs) {
        throw exceptions::SerializationException("Failed to open file for saving: " + filename);
    }
    ofs << j.dump(4);
}

void Database::load_from_file(const std::string& filename) {
    std::ifstream ifs(filename, std::ios::binary);
    if (!ifs) {
        throw exceptions::SerializationException("Failed to open file for loading: " + filename);
    }
    json j;
    ifs >> j;
    if (!j.contains("tables")) {
        throw exceptions::SerializationException("Invalid database file format.");
    }
    for (const auto& table_json : j["tables"]) {
        Table table = Table::from_json(table_json);
        auto table_ptr = std::make_shared<Table>(std::move(table));
        tables_.emplace(table_ptr->get_name(), table_ptr);
    }
}

void Database::create_index(const std::string& table_name, const std::string& index_type, const std::vector<std::string>& columns) {
    auto table = get_table(table_name);
    table->add_index(index_type, columns);
}

std::string Database::to_string() const {
    std::string db_str = "Database:\n";
    for (const auto& [name, table] : tables_) {
        db_str += table->to_string() + "\n";
    }
    return db_str;
}

QueryResult Database::execute(const std::string& query) {
    try {
        auto parsed_query = parser_.parse(query);
        return executor_.execute(parsed_query, *this);
    }
    catch (const std::invalid_argument& e) {
        return QueryResult(e.what());
    }
    catch (const exceptions::DatabaseException& e) {
        return QueryResult(e.what());
    }
    catch (const std::exception& e) {
        return QueryResult(std::string("Standard exception: ") + e.what());
    }
    catch (...) {
        return QueryResult("Unknown error during query execution.");
    }
}

} // namespace core
} // namespace memdb