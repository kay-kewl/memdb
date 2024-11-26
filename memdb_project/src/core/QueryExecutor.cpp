#include "memdb/core/Database.h"
#include "memdb/core/QueryExecutor.h"
#include "memdb/core/ExpressionParser.h"

#include "memdb/core/exceptions/DatabaseException.h"
#include "memdb/core/exceptions/TypeMismatchException.h"
#include "memdb/core/structs/ColumnInfo.h"

#include <algorithm>
#include <iostream>
#include <regex>

namespace memdb {
namespace core {

using json = nlohmann::json;

QueryResult QueryExecutor::execute(const ParsedQuery& parsed_query, Database& db) {
    switch (parsed_query.type) {
        case ParsedQuery::QueryType::CreateTable:
            try {
                db.create_table(parsed_query.table_name, parsed_query.columns);
                return QueryResult();
            }
            catch (const exceptions::TypeMismatchException& e) {
                return QueryResult(e.what());
            }
            catch (const exceptions::DatabaseException& e) {
                return QueryResult(e.what());
            }
            catch (const std::exception& e) {
                return QueryResult(e.what());
            }
            catch (...) {
                return QueryResult("Unknown error during query execution.");
            }
            break;
        
        case ParsedQuery::QueryType::Insert:
            try {
                std::shared_ptr<Table> table = db.get_table(parsed_query.table_name);
                core::RowID new_row_id = table->insert_row(*(parsed_query.insert_values));
                std::vector<std::vector<std::optional<Value>>> data = { { Value(static_cast<int32_t>(new_row_id)) } };
                return QueryResult(data);
            }
            catch (const exceptions::DatabaseException& e) {
                return QueryResult(e.what());
            }
            break;
        
        case ParsedQuery::QueryType::Select:
            return execute_select(parsed_query, db);
            break;
        
        case ParsedQuery::QueryType::Update:
            return execute_update(parsed_query, db);
            break;
        
        case ParsedQuery::QueryType::Delete:
            return execute_delete(parsed_query, db);
            break;
        
        case ParsedQuery::QueryType::CreateIndex:
            return execute_create_index(parsed_query, db);
            break;
        
        default:
            return QueryResult("Unsupported query type.");
    }
}

QueryResult QueryExecutor::execute_select(const ParsedQuery& pq, Database& db) {
    try {
        std::vector<std::vector<std::optional<Value>>> results;
        std::vector<ColumnInfo> result_columns;

        if (!pq.joins.empty()) {
            auto table1 = db.get_table(pq.table_name);
            auto table2 = db.get_table(pq.joins[0].table_name);

            const auto& columns1 = table1->get_columns();
            const auto& columns2 = table2->get_columns();

            std::vector<ColumnInfo> aliased_columns1;
            for (const auto& col : columns1) {
                std::string aliased_name = pq.table_name + "." + col.get_name();
                aliased_columns1.emplace_back(ColumnInfo(aliased_name, col.get_type()));
                result_columns.emplace_back(ColumnInfo(aliased_name, col.get_type()));
            }

            std::vector<ColumnInfo> aliased_columns2;
            for (const auto& col : columns2) {
                std::string aliased_name = pq.joins[0].table_name + "." + col.get_name();
                aliased_columns2.emplace_back(ColumnInfo(aliased_name, col.get_type()));
                result_columns.emplace_back(ColumnInfo(aliased_name, col.get_type()));
            }

            for (const auto& [row_id1, row1] : table1->get_all_rows()) {
                std::unordered_map<std::string, Value> row_map;
                for (size_t i = 0; i < columns1.size(); ++i) {
                    if (row1.get_values()[i].has_value()) {
                        std::string col_name = aliased_columns1[i].get_name();
                        row_map[pq.table_name + "." + col_name] = *(row1.get_values()[i]);
                    }
                }

                for (const auto& [row_id2, row2] : table2->get_all_rows()) {
                    std::unordered_map<std::string, Value> combined_row_map = row_map; 
                    for (size_t i = 0; i < columns2.size(); ++i) {
                        if (row2.get_values()[i].has_value()) {
                            std::string col_name = aliased_columns2[i].get_name();
                            combined_row_map[pq.joins[0].table_name + "." + col_name] = *(row2.get_values()[i]);
                        }
                    }

                    Value join_cond_value = pq.joins[0].join_condition->evaluate(combined_row_map);
                    if (join_cond_value.get_type() != Type::Bool) {
                        throw std::invalid_argument("JOIN condition does not evaluate to a boolean.");
                    }
                    if (join_cond_value.get_bool()) {
                        bool where_match = true;
                        if (pq.where_clause) {
                            Value where_cond = pq.where_clause->evaluate(combined_row_map);
                            if (where_cond.get_type() != Type::Bool) {
                                throw std::invalid_argument("WHERE clause does not evaluate to a boolean.");
                            }
                            where_match = where_cond.get_bool();
                        }
                        if (where_match) {
                            std::vector<std::optional<Value>> selected_values;
                            for (const auto& select_item : pq.select_items) {
                                try {
                                    Value val = select_item.expression->evaluate(row_map);
                                    selected_values.push_back(val);
                                } catch (const std::exception& e) {
                                    return QueryResult("Error evaluating expression in SELECT clause: " + std::string(e.what()));
                                }
                            }
                            results.push_back(selected_values);
                        }
                    }
                }
            }

            return QueryResult(results, result_columns);
        } else {
            auto table = db.get_table(pq.table_name);
            const auto& columns = table->get_columns();

            std::vector<std::vector<std::optional<Value>>> results;

            for (const auto& select_item : pq.select_items) {

                DataType expr_type = select_item.expression->get_type(); 

                result_columns.emplace_back(ColumnInfo(select_item.alias, expr_type));
            }

            for (const auto& [row_id, row] : table->get_all_rows()) {
                std::unordered_map<std::string, Value> row_map;
                for (size_t i = 0; i < columns.size(); ++i) {
                    if (row.get_values()[i].has_value()) {
                        row_map[columns[i].get_name()] = *(row.get_values()[i]);
                    }
                }

                bool match = true;
                if (pq.where_clause) {
                    Value cond = pq.where_clause->evaluate(row_map);
                    if (cond.get_type() != Type::Bool) {
                        throw std::invalid_argument("WHERE clause does not evaluate to a boolean.");
                    }
                    match = cond.get_bool();
                }

                if (match) {
                    std::vector<std::optional<Value>> selected_values;
                    for (const auto& select_item : pq.select_items) {
                        try {
                            Value val = select_item.expression->evaluate(row_map);
                            selected_values.push_back(val);
                        } catch (const std::exception& e) {
                            return QueryResult("Error evaluating expression in SELECT clause: " + std::string(e.what()));
                        }
                    }
                    results.push_back(selected_values);
                }
            }

            return QueryResult(results, result_columns);
        }
    } catch (const exceptions::DatabaseException& e) {
        return QueryResult(e.what());
    } catch (const std::exception& e) {
        return QueryResult(e.what());
    }
}

QueryResult QueryExecutor::execute_update(const ParsedQuery& pq, Database& db) {
    try {
        std::shared_ptr<Table> table = db.get_table(pq.table_name);
        const auto& columns = table->get_columns();

        size_t updated_count = 0;

        for (auto& [row_id, row] : table->get_all_rows()) {
            std::unordered_map<std::string, Value> row_map;
            for (size_t i = 0; i < columns.size(); ++i) {
                if (row.get_values()[i].has_value()) {
                    row_map[columns[i].get_name()] = *(row.get_values()[i]);
                }
            }

            bool match = true;
            if (pq.update_where_clause) {
                Value cond = pq.update_where_clause->evaluate(row_map);
                if (cond.get_type() != Type::Bool) {
                    throw std::invalid_argument("WHERE clause does not evaluate to a boolean.");
                }
                match = cond.get_bool();
            }

            if (match) {
                for (const auto& [col_name, expr] : pq.update_assignments) {
                    size_t col_index = table->get_column_index(col_name);
                    const Column& column = columns[col_index];
                    Value new_val = expr->evaluate(row_map);

                    if (new_val.get_type() != column.get_type().get_type()) {
                        throw exceptions::TypeMismatchException("Type mismatch in SET assignment for column '" + col_name + "'.");
                    }

                    row.set_value(col_index, new_val);
                    row_map[col_name] = new_val;
                }
                updated_count++;
            }
        }

        std::vector<std::vector<std::optional<Value>>> data = { { Value(static_cast<int32_t>(updated_count)) } };
        return QueryResult(data);
    }
    catch (const exceptions::DatabaseException& e) {
        return QueryResult(e.what());
    }
}

QueryResult QueryExecutor::execute_delete(const ParsedQuery& pq, Database& db) {
    try {
        std::shared_ptr<Table> table = db.get_table(pq.table_name);
        const auto& columns = table->get_columns();

        std::vector<core::RowID> rows_to_delete;

        for (const auto& [row_id, row] : table->get_all_rows()) {
            std::unordered_map<std::string, Value> row_map;
            for (size_t i = 0; i < columns.size(); ++i) {
                if (row.get_values()[i].has_value()) {
                    row_map[columns[i].get_name()] = *(row.get_values()[i]);
                }
            }

            bool match = true;
            if (pq.delete_where_clause) {
                Value cond = pq.delete_where_clause->evaluate(row_map);
                if (cond.get_type() != Type::Bool) {
                    throw std::invalid_argument("WHERE clause does not evaluate to a boolean.");
                }
                match = cond.get_bool();
            }

            if (match) {
                rows_to_delete.emplace_back(row_id);
            }
        }

        for (const auto& row_id : rows_to_delete) {
            table->delete_row(row_id);
        }

        std::vector<std::vector<std::optional<Value>>> data = { { Value(static_cast<int32_t>(rows_to_delete.size())) } };
        return QueryResult(data);
    }
    catch (const exceptions::DatabaseException& e) {
        return QueryResult(e.what());
    }
}

QueryResult QueryExecutor::execute_create_index(const ParsedQuery& pq, Database& db) {
    try {
        db.create_index(pq.table_name, pq.index_type, pq.index_columns);
        return QueryResult();
    }
    catch (const exceptions::DatabaseException& e) {
        return QueryResult(e.what());
    }
    catch (const std::exception& e) {
        return QueryResult(e.what());
    }
}

} // namespace core
} // namespace memdb