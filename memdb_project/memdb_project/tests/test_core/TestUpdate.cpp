#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "memdb/core/Database.h"
#include "memdb/core/QueryParser.h"

TEST(UpdateTest, SimpleUpdateSingleColumn) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_query = "create table users ({key, autoincrement} id : int32, name: string[32], age: int32, is_admin: bool = false);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);

    std::string insert_query1 = "insert (, \"Alice\", 30, true) to users;";
    std::string insert_query2 = "insert (, \"Bob\", 25) to users;";
    ASSERT_NO_THROW({
        memdb::core::ParsedQuery pq_insert1 = parser.parse(insert_query1);
        db.insert_row(pq_insert1.table_name, *pq_insert1.insert_values);

        memdb::core::ParsedQuery pq_insert2 = parser.parse(insert_query2);
        db.insert_row(pq_insert2.table_name, *pq_insert2.insert_values);
    });

    std::string update_query = "update users set age = 35 where name = \"Bob\";";
    memdb::core::QueryResult result = db.execute(update_query);

    ASSERT_TRUE(result.is_ok());

    ASSERT_EQ(result.get_data().size(), 1);
    EXPECT_EQ(result.get_data()[0][0]->get_int(), 1); 

    std::string select_query = "select name, age from users where name = \"Bob\";";
    memdb::core::QueryResult select_result = db.execute(select_query);

    ASSERT_TRUE(select_result.is_ok());
    ASSERT_EQ(select_result.get_data().size(), 1);
    EXPECT_EQ(select_result.get_data()[0][0]->get_string(), "Bob");
    EXPECT_EQ(select_result.get_data()[0][1]->get_int(), 35);
}

TEST(UpdateTest, UpdateMultipleColumns) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_query = "create table employees ({key, autoincrement} emp_id : int32, name: string[32], salary: int32, department: string[20], active: bool = true);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);

    std::vector<std::string> insert_queries = {
        "insert (, \"Alice\", 70000, \"Engineering\", true) to employees;",
        "insert (, \"Bob\", 50000, \"Marketing\") to employees;",
        "insert (, \"Charlie\", 60000, \"Engineering\") to employees;"
    };
    for (const auto& query : insert_queries) {
        memdb::core::ParsedQuery pq_insert = parser.parse(query);
        db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
    }

    std::string update_query = "update employees set salary = salary + 5000, active = false where department = \"Marketing\";";
    memdb::core::QueryResult result = db.execute(update_query);

    ASSERT_TRUE(result.is_ok());

    ASSERT_EQ(result.get_data().size(), 1);
    EXPECT_EQ(result.get_data()[0][0]->get_int(), 1); 

    std::string select_query = "select name, salary, active from employees where name = \"Bob\";";
    memdb::core::QueryResult select_result = db.execute(select_query);

    ASSERT_TRUE(select_result.is_ok());
    ASSERT_EQ(select_result.get_data().size(), 1);
    EXPECT_EQ(select_result.get_data()[0][0]->get_string(), "Bob");
    EXPECT_EQ(select_result.get_data()[0][1]->get_int(), 55000);
    EXPECT_EQ(select_result.get_data()[0][2]->get_bool(), false);
}

TEST(UpdateTest, UpdateWithExpressions) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_query = "create table accounts ({key, autoincrement} account_id : int32, balance: int32, bonus: int32 = 0);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);

    std::string insert_query = "insert (, 1000) to accounts;";
    memdb::core::ParsedQuery pq_insert = parser.parse(insert_query);
    db.insert_row(pq_insert.table_name, *pq_insert.insert_values);

    std::string update_query = "update accounts set balance = balance - 200, bonus = bonus + 50 where balance >= 800;";
    memdb::core::QueryResult result = db.execute(update_query);

    ASSERT_TRUE(result.is_ok());

    ASSERT_EQ(result.get_data().size(), 1);
    EXPECT_EQ(result.get_data()[0][0]->get_int(), 1);

    std::string select_query_check = "select balance, bonus from accounts where account_id = 1;";
    memdb::core::QueryResult select_result = db.execute(select_query_check);

    ASSERT_TRUE(select_result.is_ok());
    ASSERT_EQ(select_result.get_data().size(), 1);
    EXPECT_EQ(select_result.get_data()[0][0]->get_int(), 800);
    EXPECT_EQ(select_result.get_data()[0][1]->get_int(), 50);
}

TEST(UpdateTest, UpdateWithWhereCondition) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_query = "create table products ({key} product_id : int32, name: string[30], price: int32, stock: int32);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);

    std::vector<std::string> insert_queries = {
        "insert (1, \"Laptop\", 1500, 10) to products;",
        "insert (2, \"Smartphone\", 800, 20) to products;",
        "insert (3, \"Tablet\", 600, 15) to products;"
    };
    for (const auto& query : insert_queries) {
        memdb::core::ParsedQuery pq_insert = parser.parse(query);
        db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
    }

    std::string update_query = "update products set stock = stock - 5 where price > 700 && stock >= 15;";
    memdb::core::QueryResult result = db.execute(update_query);

    ASSERT_TRUE(result.is_ok());

    ASSERT_EQ(result.get_data().size(), 1);
    EXPECT_EQ(result.get_data()[0][0]->get_int(), 1);

    std::string select_query_check = "select name, stock from products where product_id = 2;";
    memdb::core::QueryResult select_result = db.execute(select_query_check);

    ASSERT_TRUE(select_result.is_ok());
    ASSERT_EQ(select_result.get_data().size(), 1);
    EXPECT_EQ(select_result.get_data()[0][0]->get_string(), "Smartphone");
    EXPECT_EQ(select_result.get_data()[0][1]->get_int(), 15);
}

TEST(UpdateTest, UpdateNonExistentTable) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string update_query = "update nonexistent set value = 10 where id = 1;";
    memdb::core::QueryResult result = db.execute(update_query);

    ASSERT_FALSE(result.is_ok());
    EXPECT_THAT(result.get_error(), ::testing::HasSubstr("Table not found: nonexistent"));
}

TEST(UpdateTest, UpdateNonExistentColumn) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_query = "create table inventory ({key} item_id : int32, item_name: string[30], quantity: int32);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);

    std::string insert_query = "insert (1, \"Widget\", 100) to inventory;";
    memdb::core::ParsedQuery pq_insert = parser.parse(insert_query);
    db.insert_row(pq_insert.table_name, *pq_insert.insert_values);

    std::string update_query = "update inventory set price = 50 where item_id = 1;";
    memdb::core::QueryResult result = db.execute(update_query);

    ASSERT_FALSE(result.is_ok());
    EXPECT_THAT(result.get_error(), ::testing::HasSubstr("Column not found: price"));
}

TEST(UpdateTest, UpdateWithTypeMismatch) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_query = "create table users ({key, autoincrement} id : int32, name: string[32], age: int32, is_admin: bool = false);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);

    std::string insert_query = "insert (, \"Alice\", 30, true) to users;";
    memdb::core::ParsedQuery pq_insert = parser.parse(insert_query);
    db.insert_row(pq_insert.table_name, *pq_insert.insert_values);

    std::string update_query = "update users set age = \"thirty-five\" where name = \"Alice\";";
    memdb::core::QueryResult result = db.execute(update_query);

    ASSERT_FALSE(result.is_ok());
    EXPECT_THAT(result.get_error(), ::testing::HasSubstr("Type mismatch in SET assignment for column \"age\"."));
}

TEST(UpdateTest, UpdateAutoIncrementColumn) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_query = "create table tickets ({key, autoincrement} ticket_id : int32, issue: string[50], status: string[20] = \"open\");";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);

    std::string insert_query = "insert (, \"Issue with login\") to tickets;";
    memdb::core::ParsedQuery pq_insert = parser.parse(insert_query);
    db.insert_row(pq_insert.table_name, *pq_insert.insert_values);

    std::string update_query = "update tickets set ticket_id = 100 where issue = \"Issue with login\";";
    memdb::core::QueryResult result = db.execute(update_query);

    ASSERT_FALSE(result.is_ok());
    EXPECT_THAT(result.get_error(), ::testing::HasSubstr("Cannot update auto-increment column \"ticket_id\"."));
}

TEST(UpdateTest, UpdateWithMultipleWhereConditions) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_query = "create table orders ({key} order_id : int32, customer: string[30], amount: int32, status: string[20]);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);

    std::vector<std::string> insert_queries = {
        "insert (101, \"Alice\", 250, \"pending\") to orders;",
        "insert (102, \"Bob\", 450, \"confirmed\") to orders;",
        "insert (103, \"Charlie\", 150, \"pending\") to orders;",
        "insert (104, \"Diana\", 500, \"shipped\") to orders;"
    };
    for (const auto& query : insert_queries) {
        memdb::core::ParsedQuery pq_insert = parser.parse(query);
        db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
    }

    std::string update_query = "update orders set status = \"processed\", amount = amount + 50 where status = \"pending\" && amount < 200;";
    memdb::core::QueryResult result = db.execute(update_query);

    ASSERT_TRUE(result.is_ok());

    ASSERT_EQ(result.get_data().size(), 1);
    EXPECT_EQ(result.get_data()[0][0]->get_int(), 1);

    std::string select_query_check = "select order_id, amount, status from orders where order_id = 103;";
    memdb::core::QueryResult select_result = db.execute(select_query_check);

    ASSERT_TRUE(select_result.is_ok());
    ASSERT_EQ(select_result.get_data().size(), 1);
    EXPECT_EQ(select_result.get_data()[0][0]->get_int(), 103);
    EXPECT_EQ(select_result.get_data()[0][1]->get_int(), 200);
    EXPECT_EQ(select_result.get_data()[0][2]->get_string(), "processed");
}

TEST(UpdateTest, UpdateWithNoMatchingRows) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_query = "create table inventory ({key} item_id : int32, item_name: string[30], quantity: int32);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);

    std::string insert_query = "insert (1, \"Widget\", 100) to inventory;";
    memdb::core::ParsedQuery pq_insert = parser.parse(insert_query);
    db.insert_row(pq_insert.table_name, *pq_insert.insert_values);

    std::string update_query = "update inventory set quantity = quantity + 10 where item_name = \"Gadget\";";
    memdb::core::QueryResult result = db.execute(update_query);

    ASSERT_TRUE(result.is_ok());

    ASSERT_EQ(result.get_data().size(), 1);
    EXPECT_EQ(result.get_data()[0][0]->get_int(), 0);

    std::string select_query_check = "select quantity from inventory where item_id = 1;";
    memdb::core::QueryResult select_result = db.execute(select_query_check);

    ASSERT_TRUE(select_result.is_ok());
    ASSERT_EQ(select_result.get_data().size(), 1);
    EXPECT_EQ(select_result.get_data()[0][0]->get_int(), 100);
}

TEST(UpdateTest, UpdateWithIndex) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_query = "create table employees ({key, autoincrement} emp_id : int32, name: string[30], department: string[20], salary: int32);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);

    std::string create_index_query = "create ordered index on employees by department;";
    memdb::core::ParsedQuery pq_create_index = parser.parse(create_index_query);
    db.execute(create_index_query);

    std::vector<std::string> insert_queries = {
        "insert (, \"Alice\", \"Engineering\", 70000) to employees;",
        "insert (, \"Bob\", \"Marketing\", 50000) to employees;",
        "insert (, \"Charlie\", \"Engineering\", 60000) to employees;",
        "insert (, \"Diana\", \"HR\", 55000) to employees;"
    };
    for (const auto& query : insert_queries) {
        memdb::core::ParsedQuery pq_insert = parser.parse(query);
        db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
    }

    std::string update_query = "update employees set salary = salary + 5000 where department = \"Engineering\";";
    memdb::core::QueryResult result = db.execute(update_query);

    ASSERT_TRUE(result.is_ok());

    ASSERT_EQ(result.get_data().size(), 1);
    EXPECT_EQ(result.get_data()[0][0]->get_int(), 2);

    std::string select_query_check = "select name, salary from employees where department = \"Engineering\";";
    memdb::core::QueryResult select_result = db.execute(select_query_check);

    ASSERT_TRUE(select_result.is_ok());
    ASSERT_EQ(select_result.get_data().size(), 2);
    EXPECT_EQ(select_result.get_data()[0][0]->get_string(), "Alice");
    EXPECT_EQ(select_result.get_data()[0][1]->get_int(), 75000);
    EXPECT_EQ(select_result.get_data()[1][0]->get_string(), "Charlie");
    EXPECT_EQ(select_result.get_data()[1][1]->get_int(), 65000);
}

TEST(UpdateTest, UpdateWithAutoincrementConflict) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_query = "create table tickets ({key, autoincrement} ticket_id : int32, description: string[50], status: string[20] = \"open\");";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);

    std::string insert_query = "insert (, \"Issue with login\") to tickets;";
    memdb::core::ParsedQuery pq_insert = parser.parse(insert_query);
    db.insert_row(pq_insert.table_name, *pq_insert.insert_values);

    std::string update_query = "update tickets set ticket_id = 100 where description = \"Issue with login\";";
    memdb::core::QueryResult result = db.execute(update_query);

    ASSERT_FALSE(result.is_ok());
    EXPECT_THAT(result.get_error(), ::testing::HasSubstr("Cannot update auto-increment column \"ticket_id\"."));
}

TEST(UpdateTest, UpdateWithDefaultValue) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_query = "create table devices ({key} device_id : int32, name: string[30], status: string[20] = \"offline\");";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);

    std::string insert_query = "insert (1, \"Router\") to devices;";
    memdb::core::ParsedQuery pq_insert = parser.parse(insert_query);
    db.insert_row(pq_insert.table_name, *pq_insert.insert_values);

    std::string update_query = "update devices set name = \"Main Router\" where device_id = 1;";
    memdb::core::QueryResult result = db.execute(update_query);

    ASSERT_TRUE(result.is_ok());

    std::string select_query = "select name, status from devices where device_id = 1;";
    memdb::core::QueryResult select_result = db.execute(select_query);

    ASSERT_TRUE(select_result.is_ok());
    ASSERT_EQ(select_result.get_data().size(), 1);
    EXPECT_EQ(select_result.get_data()[0][0]->get_string(), "Main Router");
    EXPECT_EQ(select_result.get_data()[0][1]->get_string(), "offline");
}

TEST(UpdateTest, UpdateWithComplexWhereCondition) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_query = "create table projects ({key} project_id : int32, name: string[30], budget: int32, completed: bool = false);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);

    std::vector<std::string> insert_queries = {
        "insert (101, \"Project Alpha\", 100000, true) to projects;",
        "insert (102, \"Project Beta\", 150000) to projects;",
        "insert (103, \"Project Gamma\", 200000, true) to projects;",
        "insert (104, \"Project Delta\", 120000) to projects;"
    };
    for (const auto& query : insert_queries) {
        memdb::core::ParsedQuery pq_insert = parser.parse(query);
        db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
    }

    std::string update_query = "update projects set budget = budget - 10000 where (completed = true || budget > 150000) && name != \"Project Gamma\";";
    memdb::core::QueryResult result = db.execute(update_query);

    ASSERT_TRUE(result.is_ok());

    ASSERT_EQ(result.get_data().size(), 1);
    EXPECT_EQ(result.get_data()[0][0]->get_int(), 1);

    std::string select_query_check = "select name, budget, completed from projects where project_id = 101;";
    memdb::core::QueryResult select_result = db.execute(select_query_check);

    ASSERT_TRUE(select_result.is_ok());
    ASSERT_EQ(select_result.get_data().size(), 1);
    EXPECT_EQ(select_result.get_data()[0][0]->get_string(), "Project Alpha");
    EXPECT_EQ(select_result.get_data()[0][1]->get_int(), 90000);
    EXPECT_EQ(select_result.get_data()[0][2]->get_bool(), true);
}

TEST(UpdateTest, UpdateWithIndexAndWhereCondition) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_query = "create table inventory ({key, autoincrement} item_id : int32, name: string[30], category: string[20], stock: int32);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);

    std::string create_index_query = "create ordered index on inventory by category;";
    memdb::core::ParsedQuery pq_create_index = parser.parse(create_index_query);
    db.execute(create_index_query);

    std::vector<std::string> insert_queries = {
        "insert (, \"Widget\", \"Gadgets\", 50) to inventory;",
        "insert (, \"Gizmo\", \"Gadgets\", 30) to inventory;",
        "insert (, \"Thingamajig\", \"Tools\", 20) to inventory;"
    };
    for (const auto& query : insert_queries) {
        memdb::core::ParsedQuery pq_insert = parser.parse(query);
        db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
    }

    std::string update_query = "update inventory set stock = stock - 10 where category = \"Gadgets\" && stock >= 30;";
    memdb::core::QueryResult result = db.execute(update_query);

    ASSERT_TRUE(result.is_ok());

    ASSERT_EQ(result.get_data().size(), 1);
    EXPECT_EQ(result.get_data()[0][0]->get_int(), 2);

    std::string select_query = "select name, stock from inventory where category = \"Gadgets\";";
    memdb::core::QueryResult select_result = db.execute(select_query);

    ASSERT_TRUE(select_result.is_ok());
    ASSERT_EQ(select_result.get_data().size(), 2);
    EXPECT_EQ(select_result.get_data()[0][0]->get_string(), "Widget");
    EXPECT_EQ(select_result.get_data()[0][1]->get_int(), 40);
    EXPECT_EQ(select_result.get_data()[1][0]->get_string(), "Gizmo");
    EXPECT_EQ(select_result.get_data()[1][1]->get_int(), 20);
}

TEST(UpdateTest, UpdateWithOverwriteDefaultValue) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_query = "create table settings ({key} setting_id : int32, key_name: string[30], value: string[50] = \"default\");";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);

    std::string insert_query = "insert (1, \"theme\") to settings;";
    memdb::core::ParsedQuery pq_insert = parser.parse(insert_query);
    db.insert_row(pq_insert.table_name, *pq_insert.insert_values);

    std::string update_query = "update settings set value = \"dark\" where key_name = \"theme\";";
    memdb::core::QueryResult result = db.execute(update_query);

    ASSERT_TRUE(result.is_ok());

    std::string select_query = "select key_name, value from settings where setting_id = 1;";
    memdb::core::QueryResult select_result = db.execute(select_query);

    ASSERT_TRUE(select_result.is_ok());
    ASSERT_EQ(select_result.get_data().size(), 1);
    EXPECT_EQ(select_result.get_data()[0][0]->get_string(), "theme");
    EXPECT_EQ(select_result.get_data()[0][1]->get_string(), "dark");
}

TEST(UpdateTest, UpdateWithInvalidExpressions) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_query = "create table metrics (metric_id : int32, value1: int32, value2: int32);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);

    std::string insert_query = "insert (1, 10, 20) to metrics;";
    memdb::core::ParsedQuery pq_insert = parser.parse(insert_query);
    db.insert_row(pq_insert.table_name, *pq_insert.insert_values);

    std::string update_query = "update metrics set value1 = value1 / (value2 - 20) where metric_id = 1;";
    memdb::core::QueryResult result = db.execute(update_query);

    ASSERT_FALSE(result.is_ok());
    EXPECT_THAT(result.get_error(), ::testing::HasSubstr("Division by zero"));
}

TEST(UpdateTest, UpdateWithNoSetClause) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string update_query = "update users where id = 1;";
    memdb::core::QueryResult result = db.execute(update_query);

    ASSERT_FALSE(result.is_ok());
    EXPECT_THAT(result.get_error(), ::testing::HasSubstr("Expected \"set\" after table name in UPDATE."));
}

TEST(UpdateTest, UpdateWithEmptySetClause) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);
    
    std::string create_query = "create table settings ({key} setting_id : int32, key_name: string[30], value: string[50] = \"default\");";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);
    
    std::string insert_query = "insert (1, \"theme\") to settings;";
    memdb::core::ParsedQuery pq_insert = parser.parse(insert_query);
    db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
    
    std::string update_query = "update settings set where setting_id = 1;";
    memdb::core::QueryResult result = db.execute(update_query);
    
    ASSERT_FALSE(result.is_ok());
    EXPECT_THAT(result.get_error(), ::testing::HasSubstr("No assignment in UPDATE"));
}

TEST(UpdateTest, UpdateWithUniqueConstraintViolation) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_query = "create table users ({key} user_id : int32, username: string[20], {unique} email: string[50]);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);

    std::string insert_query1 = "insert (1, \"alice\", \"alice@example.com\") to users;";
    std::string insert_query2 = "insert (2, \"bob\", \"bob@example.com\") to users;";
    memdb::core::ParsedQuery pq_insert = parser.parse(insert_query1);
    db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
    pq_insert = parser.parse(insert_query2);
    db.insert_row(pq_insert.table_name, *pq_insert.insert_values);

    std::string update_query = "update users set email = \"alice@example.com\" where username = \"bob\";";
    memdb::core::QueryResult result = db.execute(update_query);

    ASSERT_FALSE(result.is_ok());
    EXPECT_THAT(result.get_error(), ::testing::HasSubstr("Duplicate value for unique/key column"));
}

TEST(UpdateTest, UpdateStringExceedsMaxLength) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);
    
    std::string create_query = "create table test_table ({key} id : int32, code: string[5]);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);
    
    std::string insert_query = "insert (1, \"12345\") to test_table;";
    memdb::core::ParsedQuery pq_insert = parser.parse(insert_query);
    db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
    
    std::string update_query = "update test_table set code = \"123456\" where id = 1;";
    memdb::core::QueryResult result = db.execute(update_query);
    
    ASSERT_FALSE(result.is_ok());
    EXPECT_THAT(result.get_error(), ::testing::HasSubstr("Value for column \"code\" exceeds maximum length of 5."));
}

TEST(UpdateTest, UpdateMultipleRows) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);
    
    std::string create_query = "create table inventory ({key} item_id : int32, quantity: int32);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);
    
    std::vector<std::string> insert_queries = {
        "insert (1, 50) to inventory;",
        "insert (2, 30) to inventory;",
        "insert (3, 20) to inventory;"
    };
    for (const auto& query : insert_queries) {
        memdb::core::ParsedQuery pq_insert = parser.parse(query);
        db.insert_row("inventory", *pq_insert.insert_values);
    }
    
    std::string update_query = "update inventory set quantity = 0 where quantity <= 30;";
    memdb::core::QueryResult result = db.execute(update_query);
    
    ASSERT_TRUE(result.is_ok());
    
    std::string select_query = "select item_id, quantity from inventory;";
    memdb::core::QueryResult select_result = db.execute(select_query);
    
    ASSERT_TRUE(select_result.is_ok());
    ASSERT_EQ(select_result.get_data().size(), 3);
    
    EXPECT_EQ(select_result.get_data()[0][1]->get_int(), 50);
    EXPECT_EQ(select_result.get_data()[1][1]->get_int(), 0);
    EXPECT_EQ(select_result.get_data()[2][1]->get_int(), 0);
}

TEST(UpdateTest, UpdateWithComplexExpressions) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);
    
    std::string create_query = "create table finances ({key} account_id : int32, balance: int32, bonus: int32 = 0);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);
    
    std::string insert_query = "insert (1, 1000) to finances;";
    parser.parse(insert_query);
    db.insert_row("finances", {1, 1000});
    
    std::string update_query = "update finances set balance = balance + (balance / 10), bonus = bonus + 100 where account_id = 1;";
    memdb::core::QueryResult result = db.execute(update_query);
    
    ASSERT_TRUE(result.is_ok());
    
    std::string select_query = "select balance, bonus from finances where account_id = 1;";
    memdb::core::QueryResult select_result = db.execute(select_query);
    
    ASSERT_TRUE(select_result.is_ok());
    ASSERT_EQ(select_result.get_data().size(), 1);
    EXPECT_EQ(select_result.get_data()[0][0]->get_int(), 1100);
    EXPECT_EQ(select_result.get_data()[0][1]->get_int(), 100);
}

TEST(UpdateTest, UpdateWithWhereConditionUsingLength) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);
    
    std::string create_query = "create table documents ({key} doc_id : int32, title: string[50], content: string[200]);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);
    
    std::vector<std::string> insert_queries = {
        "insert (1, \"Short Title\", \"This is a short document.\") to documents;",
        "insert (2, \"A Very Long Title That Exceeds Normal Length\", \"This document has a lengthy content.\") to documents;",
        "insert (3, \"Medium Title\", \"Content is of medium length.\") to documents;"
    };
    for (const auto& query : insert_queries) {
        memdb::core::ParsedQuery pq_insert = parser.parse(query);
        db.insert_row("documents", *pq_insert.insert_values);
    }
    
    std::string update_query = "update documents set content = \"Updated content.\" where |title| > 20;";
    memdb::core::QueryResult result = db.execute(update_query);
    
    ASSERT_TRUE(result.is_ok());
    
    std::string select_query = "select title, content from documents where |title| > 20;";
    memdb::core::QueryResult select_result = db.execute(select_query);
    
    ASSERT_TRUE(select_result.is_ok());
    ASSERT_EQ(select_result.get_data().size(), 1);
    EXPECT_EQ(select_result.get_data()[0][0]->get_string(), "A Very Long Title That Exceeds Normal Length");
    EXPECT_EQ(select_result.get_data()[0][1]->get_string(), "Updated content.");
}

TEST(UpdateTest, UpdateWithoutWhereClause) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);
    
    std::string create_query = "create table devices ({key} device_id : int32, status: string[20] = \"inactive\");";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);
    
    std::vector<std::string> insert_queries = {
        "insert (1) to devices;",
        "insert (2) to devices;",
        "insert (3, \"active\") to devices;"
    };
    for (const auto& query : insert_queries) {
        memdb::core::ParsedQuery pq_insert = parser.parse(query);
        db.insert_row("devices", *pq_insert.insert_values);
    }
    
    std::string update_query = "update devices set status = \"active\";";
    memdb::core::QueryResult result = db.execute(update_query);
    
    ASSERT_TRUE(result.is_ok());
    
    std::string select_query = "select device_id, status from devices;";
    memdb::core::QueryResult select_result = db.execute(select_query);
    
    ASSERT_TRUE(select_result.is_ok());
    ASSERT_EQ(select_result.get_data().size(), 3);
    
    EXPECT_EQ(select_result.get_data()[0][1]->get_string(), "active");
    EXPECT_EQ(select_result.get_data()[1][1]->get_string(), "active");
    EXPECT_EQ(select_result.get_data()[2][1]->get_string(), "active");
}

TEST(UpdateTest, UpdateWithComplexNestedExpressions) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);
    
    std::string create_query = "create table calculations ({key} calc_id : int32, a : int32, b : int32, c : int32, result : int32 = 0);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);
    
    std::string insert_query = "insert (1, 2, 3, 4) to calculations;";
    memdb::core::ParsedQuery pq_insert = parser.parse(insert_query);
    db.insert_row("calculations", *pq_insert.insert_values);
    
    std::string update_query = "update calculations set result = (a + (b * c)) / 2 where calc_id = 1;";
    memdb::core::QueryResult result = db.execute(update_query);
    
    ASSERT_TRUE(result.is_ok());
    
    std::string select_query = "select result from calculations where calc_id = 1;";
    memdb::core::QueryResult select_result = db.execute(select_query);
    
    ASSERT_TRUE(select_result.is_ok());
    ASSERT_EQ(select_result.get_data().size(), 1);
    EXPECT_EQ(select_result.get_data()[0][0]->get_int(), 7);
}

TEST(UpdateTest, UpdateWithInvalidSetClause) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);
    
    std::string create_query = "create table items ({key} item_id : int32, name: string[30], quantity: int32);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);
    
    std::string insert_query = "insert (1, \"Widget\", 10) to items;";
    memdb::core::ParsedQuery pq_insert = parser.parse(insert_query);
    db.insert_row("items", *pq_insert.insert_values);
    
    std::string update_query = "update items set quantity 20 where item_id = 1;";
    memdb::core::QueryResult result = db.execute(update_query);
    
    ASSERT_FALSE(result.is_ok());
    EXPECT_THAT(result.get_error(), ::testing::HasSubstr("Invalid assignment in UPDATE: quantity 20"));
}