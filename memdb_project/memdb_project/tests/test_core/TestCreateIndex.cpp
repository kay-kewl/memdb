#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "memdb/core/Database.h"
#include "memdb/core/QueryParser.h"

#include <chrono>

TEST(CreateIndexTest, OrderedIndexAcceleratesSelectQueries) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_table_query = 
        "create table employees ({key, autoincrement} emp_id : int32, name: string[30], salary: int32);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_table_query);
    db.create_table(pq_create.table_name, pq_create.columns);

    const int num_records = 1000;
    for (int i = 1; i <= num_records; ++i) {
        std::string insert_query = "insert (" + std::to_string(i) + ", \"Employee" + std::to_string(i) + "\", " + std::to_string(30000 + (i % 20000)) + ") to employees;";
        memdb::core::ParsedQuery pq_insert = parser.parse(insert_query);
        db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
    }

    std::string select_query = "select emp_id, name, salary from employees where salary > 30500;";
    
    auto start_no_index = std::chrono::high_resolution_clock::now();
    memdb::core::QueryResult result_no_index = db.execute(select_query);
    auto end_no_index = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> duration_no_index = end_no_index - start_no_index;
    
    ASSERT_TRUE(result_no_index.is_ok());
    EXPECT_GT(result_no_index.get_data().size(), 0);

    std::string create_index_query = "create ordered index on employees by salary;";
    memdb::core::QueryResult index_result = db.execute(create_index_query);
    ASSERT_TRUE(index_result.is_ok()) << "Error creating index: " << index_result.get_error();

    auto start_with_index = std::chrono::high_resolution_clock::now();
    memdb::core::QueryResult result_with_index = db.execute(select_query);
    auto end_with_index = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> duration_with_index = end_with_index - start_with_index;

    ASSERT_TRUE(result_with_index.is_ok());
    EXPECT_GT(result_with_index.get_data().size(), 0);

    EXPECT_LE(duration_with_index.count(), duration_no_index.count()) 
        << "SELECT query with index took longer (" << duration_with_index.count() 
        << " ms) than without index (" << duration_no_index.count() << " ms).";
}

TEST(CreateIndexTest, UnorderedIndexAcceleratesSelectQueries) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_table_query = 
        "create table products ({key, autoincrement} product_id : int32, category: string[20], price: int32, in_stock: bool);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_table_query);
    db.create_table(pq_create.table_name, pq_create.columns);

    const int num_records = 10000;
    for (int i = 1; i <= num_records; ++i) {
        std::string category = (i % 2 == 0) ? "Electronics" : "Furniture";
        std::string in_stock = (i % 3 == 0) ? "false" : "true";
        std::string insert_query = "insert (" + std::to_string(i) + ", \"" + category + "\", " + 
                                    std::to_string(100 + (i % 500)) + ", " + in_stock + ") to products;";
        memdb::core::ParsedQuery pq_insert = parser.parse(insert_query);
        db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
    }

    std::string select_query = "select product_id, category, price, in_stock from products where category = \"Electronics\" && in_stock = true;";
    
    auto start_no_index = std::chrono::high_resolution_clock::now();
    memdb::core::QueryResult result_no_index = db.execute(select_query);
    auto end_no_index = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> duration_no_index = end_no_index - start_no_index;
    
    ASSERT_TRUE(result_no_index.is_ok());
    EXPECT_GT(result_no_index.get_data().size(), 0);

    std::string create_index_query = "create unordered index on products by category, in_stock;";
    memdb::core::QueryResult index_result = db.execute(create_index_query);
    ASSERT_TRUE(index_result.is_ok()) << "Error creating index: " << index_result.get_error();

    auto start_with_index = std::chrono::high_resolution_clock::now();
    memdb::core::QueryResult result_with_index = db.execute(select_query);
    auto end_with_index = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> duration_with_index = end_with_index - start_with_index;

    ASSERT_TRUE(result_with_index.is_ok());
    EXPECT_GT(result_with_index.get_data().size(), 0);

    EXPECT_LE(duration_with_index.count(), duration_no_index.count()) 
        << "SELECT query with index took longer (" << duration_with_index.count() 
        << " ms) than without index (" << duration_no_index.count() << " ms).";
}

TEST(CreateIndexTest, MultipleIndicesUsage) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_table_query = 
        "create table orders ({key, autoincrement} order_id : int32, customer_id : int32, amount : int32, status : string[20]);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_table_query);
    db.create_table(pq_create.table_name, pq_create.columns);

    const int num_records = 1000;
    for (int i = 1; i <= num_records; ++i) {
        int customer_id = 100 + i % 10;
        int amount = 150 + (i % 100);
        std::string status = (i % 2 == 0) ? "shipped" : "pending";
        std::string insert_query = "insert (" + std::to_string(i) + ", " + std::to_string(customer_id) + ", " + 
                                    std::to_string(amount) + ", \"" + status + "\") to orders;";
        memdb::core::ParsedQuery pq_insert = parser.parse(insert_query);
        db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
    }

    std::string create_index1 = "create ordered index on orders by amount;";
    std::string create_index2 = "create unordered index on orders by customer_id, status;";
    ASSERT_TRUE(db.execute(create_index1).is_ok()) << "Error creating index1: " << db.execute(create_index1).get_error();
    ASSERT_TRUE(db.execute(create_index2).is_ok()) << "Error creating index2: " << db.execute(create_index2).get_error();

    std::string select_query = "select order_id, customer_id, amount, status from orders where amount > 120 && customer_id > 105 && status = \"shipped\";";

    auto start_with_indices = std::chrono::high_resolution_clock::now();
    memdb::core::QueryResult result_with_indices = db.execute(select_query);
    auto end_with_indices = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> duration_with_indices = end_with_indices - start_with_indices;

    ASSERT_TRUE(result_with_indices.is_ok());
    EXPECT_GT(result_with_indices.get_data().size(), 0);
}

TEST(CreateIndexTest, IndexUsageAfterUpdatingData) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_table_query = 
        "create table employees ({key, autoincrement} emp_id : int32, name: string[30], department: string[20], salary : int32);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_table_query);
    db.create_table(pq_create.table_name, pq_create.columns);

    std::vector<std::string> insert_queries = {
        "insert (, \"Alice\", \"Engineering\", 70000) to employees;",
        "insert (, \"Bob\", \"Marketing\", 50000) to employees;",
        "insert (, \"Charlie\", \"Engineering\", 60000) to employees;",
        "insert (, \"David\", \"HR\", 55000) to employees;"
    };
    for (const auto& query : insert_queries) {
        memdb::core::ParsedQuery pq_insert = parser.parse(query);
        db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
    }

    std::string create_index_query = "create ordered index on employees by salary;";
    memdb::core::QueryResult index_result = db.execute(create_index_query);
    ASSERT_TRUE(index_result.is_ok()) << "Error creating index: " << index_result.get_error();

    std::string update_query = "update employees set salary = 80000 where name = \"Charlie\";";
    memdb::core::QueryResult update_result = db.execute(update_query);
    ASSERT_TRUE(update_result.is_ok()) << "Error updating data: " << update_result.get_error();

    std::string select_query = "select emp_id, name, salary from employees where salary > 75000;";
    
    auto start_with_index = std::chrono::high_resolution_clock::now();
    memdb::core::QueryResult result_with_index = db.execute(select_query);
    auto end_with_index = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> duration_with_index = end_with_index - start_with_index;
    
    ASSERT_TRUE(result_with_index.is_ok());
    EXPECT_EQ(result_with_index.get_data().size(), 1);
    
    std::vector<std::tuple<int, std::string, int>> expected_results = {
        {3, "Charlie", 80000}
    };

    for (const auto& expected : expected_results) {
        bool found = false;
        for (const auto& row : result_with_index.get_data()) {
            if (row[0]->get_int() == std::get<0>(expected) &&
                row[1]->get_string() == std::get<1>(expected) &&
                row[2]->get_int() == std::get<2>(expected)) {
                found = true;
                break;
            }
        }
        EXPECT_TRUE(found) << "Expected row not found: "
                           << "emp_id=" << std::get<0>(expected) << ", "
                           << "name=\"" << std::get<1>(expected) << "\", "
                           << "salary=" << std::get<2>(expected);
    }
}

TEST(CreateIndexTest, IndexConsistencyAfterDataModification) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_table_query = 
        "create table inventory ({key, autoincrement} item_id : int32, stock : int32);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_table_query);
    db.create_table(pq_create.table_name, pq_create.columns);

    for (int i = 1; i <= 100; ++i) {
        std::string insert_query = "insert (, " + std::to_string(50) + ") to inventory;";
        memdb::core::ParsedQuery pq_insert = parser.parse(insert_query);
        db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
    }

    std::string create_index_query = "create ordered index on inventory by stock;";
    ASSERT_TRUE(db.execute(create_index_query).is_ok());

    std::string update_query = "update inventory set stock = 10 where item_id <= 50;";
    ASSERT_TRUE(db.execute(update_query).is_ok());

    std::string select_query = "select item_id, stock from inventory where stock < 20;";
    memdb::core::QueryResult result = db.execute(select_query);

    ASSERT_TRUE(result.is_ok());
    EXPECT_EQ(result.get_data().size(), 50);

    for (const auto& row : result.get_data()) {
        EXPECT_LE(row[1]->get_int(), 20);
    }
}

TEST(CreateIndexTest, DuplicateValuesInIndex) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_table_query = 
        "create table duplicates_test ({key, autoincrement} id : int32, value : int32);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_table_query);
    db.create_table(pq_create.table_name, pq_create.columns);

    for (int i = 1; i <= 100; ++i) {
        int value = (i % 10);
        std::string insert_query = "insert (, " + std::to_string(value) + ") to duplicates_test;";
        memdb::core::ParsedQuery pq_insert = parser.parse(insert_query);
        db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
    }

    std::string create_index_query = "create ordered index on duplicates_test by value;";
    ASSERT_TRUE(db.execute(create_index_query).is_ok());

    std::string select_query = "select id, value from duplicates_test where value = 5;";
    memdb::core::QueryResult result = db.execute(select_query);

    ASSERT_TRUE(result.is_ok());
    EXPECT_EQ(result.get_data().size(), 10);

    for (const auto& row : result.get_data()) {
        EXPECT_EQ(row[1]->get_int(), 5);
    }
}

TEST(CreateIndexTest, CompositeIndexUsage) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_table_query = 
        "create table composite_test ({key, autoincrement} id : int32, category : string[20], subcategory : string[20], value : int32);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_table_query);
    db.create_table(pq_create.table_name, pq_create.columns);

    const char* categories[] = {"CatA", "CatB"};
    const char* subcategories[] = {"Sub1", "Sub2", "Sub3"};
    for (int i = 1; i <= 300; ++i) {
        std::string category = categories[i % 2];
        std::string subcategory = subcategories[i % 3];
        int value = i;
        std::string insert_query = "insert (, \"" + category + "\", \"" + subcategory + "\", " + std::to_string(value) + ") to composite_test;";
        memdb::core::ParsedQuery pq_insert = parser.parse(insert_query);
        db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
    }

    std::string create_index_query = "create unordered index on composite_test by category, subcategory;";
    ASSERT_TRUE(db.execute(create_index_query).is_ok());

    std::string select_query = "select id, category, subcategory, value from composite_test where category = \"CatA\" && subcategory = \"Sub2\";";

    memdb::core::QueryResult result = db.execute(select_query);

    ASSERT_TRUE(result.is_ok());

    for (const auto& row : result.get_data()) {
        EXPECT_EQ(row[1]->get_string(), "CatA");
        EXPECT_EQ(row[2]->get_string(), "Sub2");
    }
}
