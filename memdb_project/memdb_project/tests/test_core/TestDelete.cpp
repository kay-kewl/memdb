#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "memdb/core/Database.h"
#include "memdb/core/QueryParser.h"

TEST(DeleteTest, DeleteSingleRow) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_query = "create table users ({key, autoincrement} id : int32, name: string[32], email: string[50]);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);

    std::string insert_query1 = "insert (, \"Alice\", \"alice@example.com\") to users;";
    std::string insert_query2 = "insert (, \"Bob\", \"bob@example.com\") to users;";
    ASSERT_NO_THROW({
        memdb::core::ParsedQuery pq_insert1 = parser.parse(insert_query1);
        db.insert_row(pq_insert1.table_name, *pq_insert1.insert_values);

        memdb::core::ParsedQuery pq_insert2 = parser.parse(insert_query2);
        db.insert_row(pq_insert2.table_name, *pq_insert2.insert_values);
    });

    std::string delete_query = "delete users where name = \"Bob\";";
    memdb::core::QueryResult result = db.execute(delete_query);

    ASSERT_TRUE(result.is_ok());

    ASSERT_EQ(result.get_data().size(), 1);
    EXPECT_EQ(result.get_data()[0][0]->get_int(), 1);

    std::string select_query = "select id, name, email from users;";
    memdb::core::QueryResult select_result = db.execute(select_query);

    ASSERT_TRUE(select_result.is_ok());
    ASSERT_EQ(select_result.get_data().size(), 1); 

    EXPECT_EQ(select_result.get_data()[0][1]->get_string(), "Alice");
    EXPECT_EQ(select_result.get_data()[0][2]->get_string(), "alice@example.com");
}

TEST(DeleteTest, DeleteWithWhereClause) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_query = "create table products ({key} product_id : int32, name: string[50], price: int32, in_stock: bool = true);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);

    std::vector<std::string> insert_queries = {
        "insert (101, \"Laptop\", 1500, true) to products;",
        "insert (102, \"Smartphone\", 800) to products;", 
        "insert (103, \"Tablet\", 600, false) to products;",
        "insert (104, \"Monitor\", 300, true) to products;"
    };
    for (const auto& query : insert_queries) {
        ASSERT_NO_THROW({
            memdb::core::ParsedQuery pq_insert = parser.parse(query);
            db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
        });
    }

    std::string delete_query = "delete products where price < 1000;";
    memdb::core::QueryResult result = db.execute(delete_query);

    ASSERT_TRUE(result.is_ok());
    ASSERT_EQ(result.get_data().size(), 1);
    EXPECT_EQ(result.get_data()[0][0]->get_int(), 3);

    std::string select_query = "select product_id, name, price, in_stock from products;";
    memdb::core::QueryResult select_result = db.execute(select_query);

    ASSERT_TRUE(select_result.is_ok());
    ASSERT_EQ(select_result.get_data().size(), 1);

    EXPECT_EQ(select_result.get_data()[0][1]->get_string(), "Laptop");
}

TEST(DeleteTest, DeleteNonExistentRows) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_query = "create table employees ({key} emp_id : int32, name: string[30], department: string[20]);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);

    std::string insert_query = "insert (1, \"John Doe\", \"Engineering\") to employees;";
    ASSERT_NO_THROW({
        memdb::core::ParsedQuery pq_insert = parser.parse(insert_query);
        db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
    });

    std::string delete_query = "delete employees where department = \"HR\";";
    memdb::core::QueryResult result = db.execute(delete_query);

    ASSERT_TRUE(result.is_ok());
    ASSERT_EQ(result.get_data().size(), 1);
    EXPECT_EQ(result.get_data()[0][0]->get_int(), 0);
}

TEST(DeleteTest, DeleteFromEmptyTable) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_query = "create table logs ({key} log_id : int32, message: string[100]);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);

    std::string delete_query = "delete logs where log_id = 1;";
    memdb::core::QueryResult result = db.execute(delete_query);

    ASSERT_TRUE(result.is_ok());
    ASSERT_EQ(result.get_data().size(), 1);
    EXPECT_EQ(result.get_data()[0][0]->get_int(), 0);
}

TEST(DeleteTest, DeleteMultipleRows) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_query = "create table orders ({key, autoincrement} order_id : int32, customer: string[30], amount: int32);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);

    std::vector<std::string> insert_queries = {
        "insert (, \"Alice\", 250) to orders;",
        "insert (, \"Bob\", 450) to orders;",
        "insert (, \"Charlie\", 150) to orders;",
        "insert (, \"Alice\", 550) to orders;",
        "insert (, \"David\", 350) to orders;"
    };
    for (const auto& query : insert_queries) {
        ASSERT_NO_THROW({
            memdb::core::ParsedQuery pq_insert = parser.parse(query);
            db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
        });
    }

    std::string delete_query = "delete orders where customer = \"Alice\";";
    memdb::core::QueryResult result = db.execute(delete_query);

    ASSERT_TRUE(result.is_ok());
    ASSERT_EQ(result.get_data().size(), 1);
    EXPECT_EQ(result.get_data()[0][0]->get_int(), 2);

    std::string select_query = "select customer, amount from orders;";
    memdb::core::QueryResult select_result = db.execute(select_query);

    ASSERT_TRUE(select_result.is_ok());
    ASSERT_EQ(select_result.get_data().size(), 3);

    EXPECT_EQ(select_result.get_data()[0][0]->get_string(), "Bob");
    EXPECT_EQ(select_result.get_data()[0][1]->get_int(), 450);

    EXPECT_EQ(select_result.get_data()[1][0]->get_string(), "Charlie");
    EXPECT_EQ(select_result.get_data()[1][1]->get_int(), 150);

    EXPECT_EQ(select_result.get_data()[2][0]->get_string(), "David");
    EXPECT_EQ(select_result.get_data()[2][1]->get_int(), 350);
}

TEST(DeleteTest, DeleteWithIndexes) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_query = 
        "create table employees ({key, autoincrement} emp_id : int32, name: string[30], department: string[20], {unique} email: string[50]);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);

    std::string create_index = "create ordered index on employees by department;";
    memdb::core::ParsedQuery pq_index = parser.parse(create_index);
    memdb::core::QueryResult index_result = db.execute(create_index);
    ASSERT_TRUE(index_result.is_ok());

    std::vector<std::string> insert_queries = {
        "insert (, \"Alice\", \"Engineering\", \"alice@example.com\") to employees;",
        "insert (, \"Bob\", \"Marketing\", \"bob@example.com\") to employees;",
        "insert (, \"Charlie\", \"Engineering\", \"charlie@example.com\") to employees;",
        "insert (, \"David\", \"HR\", \"david@example.com\") to employees;"
    };
    for (const auto& query : insert_queries) {
        memdb::core::ParsedQuery pq_insert = parser.parse(query);
        db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
    }

    std::string delete_query = "delete employees where department = \"Engineering\";";
    memdb::core::QueryResult delete_result = db.execute(delete_query);
    ASSERT_TRUE(delete_result.is_ok());
    ASSERT_EQ(delete_result.get_data().size(), 1);
    EXPECT_EQ(delete_result.get_data()[0][0]->get_int(), 2);

    std::string select_query = "select name, department from employees;";
    memdb::core::QueryResult select_result = db.execute(select_query);
    ASSERT_TRUE(select_result.is_ok());
    ASSERT_EQ(select_result.get_data().size(), 2);

    EXPECT_EQ(select_result.get_data()[0][0]->get_string(), "Bob");
    EXPECT_EQ(select_result.get_data()[0][1]->get_string(), "Marketing");

    EXPECT_EQ(select_result.get_data()[1][0]->get_string(), "David");
    EXPECT_EQ(select_result.get_data()[1][1]->get_string(), "HR");
}

TEST(DeleteTest, DeleteAffectingSubsequentQueries) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_query = "create table inventory ({key, autoincrement} item_id : int32, item_name: string[30], quantity: int32);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);

    std::vector<std::string> insert_queries = {
        "insert (, \"Widget\", 100) to inventory;",
        "insert (, \"Gadget\", 50) to inventory;",
        "insert (, \"Thingamajig\", 75) to inventory;"
    };
    for (const auto& query : insert_queries) {
        memdb::core::ParsedQuery pq_insert = parser.parse(query);
        db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
    }

    std::string delete_query = "delete inventory where item_name = \"Widget\";";
    memdb::core::QueryResult delete_result = db.execute(delete_query);
    ASSERT_TRUE(delete_result.is_ok());
    ASSERT_EQ(delete_result.get_data().size(), 1);
    EXPECT_EQ(delete_result.get_data()[0][0]->get_int(), 1);

    std::string select_query = "select item_id, item_name, quantity from inventory;";
    memdb::core::QueryResult select_result = db.execute(select_query);
    ASSERT_TRUE(select_result.is_ok());
    ASSERT_EQ(select_result.get_data().size(), 2);

    for (const auto& row : select_result.get_data()) {
        EXPECT_NE(row[1]->get_string(), "Widget");
    }
}

TEST(DeleteTest, DeleteWithDefaultValues) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_query = "create table sensors ({key, autoincrement} sensor_id : int32, type: string[20], active: bool = true);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);

    std::string insert_query1 = "insert (, \"Temperature\") to sensors;";
    std::string insert_query2 = "insert (, \"Pressure\", false) to sensors;";
    ASSERT_NO_THROW({
        memdb::core::ParsedQuery pq_insert1 = parser.parse(insert_query1);
        db.insert_row(pq_insert1.table_name, *pq_insert1.insert_values);

        memdb::core::ParsedQuery pq_insert2 = parser.parse(insert_query2);
        db.insert_row(pq_insert2.table_name, *pq_insert2.insert_values);
    });

    std::string delete_query = "delete sensors where active = true;";
    memdb::core::QueryResult delete_result = db.execute(delete_query);
    ASSERT_TRUE(delete_result.is_ok());
    ASSERT_EQ(delete_result.get_data().size(), 1);
    EXPECT_EQ(delete_result.get_data()[0][0]->get_int(), 1);

    std::string select_query = "select sensor_id, type, active from sensors;";
    memdb::core::QueryResult select_result = db.execute(select_query);
    ASSERT_TRUE(select_result.is_ok());
    ASSERT_EQ(select_result.get_data().size(), 1);

    EXPECT_EQ(select_result.get_data()[0][1]->get_string(), "Pressure");
    EXPECT_EQ(select_result.get_data()[0][2]->get_bool(), false);
}

TEST(DeleteTest, DeleteMultipleRowsWithSameCondition) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_query = "create table books ({key, autoincrement} book_id : int32, title: string[50], author: string[30], genre: string[20]);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);

    std::vector<std::string> insert_queries = {
        "insert (, \"The Hobbit\", \"J.R.R. Tolkien\", \"Fantasy\") to books;",
        "insert (, \"Harry Potter\", \"J.K. Rowling\", \"Fantasy\") to books;",
        "insert (, \"1984\", \"George Orwell\", \"Dystopian\") to books;",
        "insert (, \"Animal Farm\", \"George Orwell\", \"Satire\") to books;",
        "insert (, \"The Lord of the Rings\", \"J.R.R. Tolkien\", \"Fantasy\") to books;"
    };
    for (const auto& query : insert_queries) {
        memdb::core::ParsedQuery pq_insert = parser.parse(query);
        db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
    }

    std::string delete_query = "delete books where genre = \"Fantasy\";";
    memdb::core::QueryResult delete_result = db.execute(delete_query);
    ASSERT_TRUE(delete_result.is_ok());
    ASSERT_EQ(delete_result.get_data().size(), 1);
    EXPECT_EQ(delete_result.get_data()[0][0]->get_int(), 3); 

    std::string select_query = "select title, genre from books;";
    memdb::core::QueryResult select_result = db.execute(select_query);
    ASSERT_TRUE(select_result.is_ok());
    ASSERT_EQ(select_result.get_data().size(), 2);

    EXPECT_EQ(select_result.get_data()[0][0]->get_string(), "1984");
    EXPECT_EQ(select_result.get_data()[0][1]->get_string(), "Dystopian");

    EXPECT_EQ(select_result.get_data()[1][0]->get_string(), "Animal Farm");
    EXPECT_EQ(select_result.get_data()[1][1]->get_string(), "Satire");
}

TEST(DeleteTest, DeleteWithIndexedColumn) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_query = "create table inventory ({key} item_id : int32, name: string[30], category: string[20], stock: int32);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);

    std::string create_index = "create ordered index on inventory by category;";
    memdb::core::ParsedQuery pq_index = parser.parse(create_index);
    memdb::core::QueryResult index_result = db.execute(create_index);
    ASSERT_TRUE(index_result.is_ok());

    std::vector<std::string> insert_queries = {
        "insert (101, \"Widget\", \"Tools\", 50) to inventory;",
        "insert (102, \"Gadget\", \"Electronics\", 30) to inventory;",
        "insert (103, \"Doohickey\", \"Tools\", 20) to inventory;",
        "insert (104, \"Thingamabob\", \"Electronics\", 60) to inventory;",
        "insert (105, \"Whatsit\", \"Gadgets\", 40) to inventory;"
    };
    for (const auto& query : insert_queries) {
        memdb::core::ParsedQuery pq_insert = parser.parse(query);
        db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
    }

    std::string delete_query = "delete inventory where category = \"Tools\";";
    memdb::core::QueryResult delete_result = db.execute(delete_query);
    ASSERT_TRUE(delete_result.is_ok());
    ASSERT_EQ(delete_result.get_data().size(), 1);
    EXPECT_EQ(delete_result.get_data()[0][0]->get_int(), 2);

    std::string select_query = "select name, category, stock from inventory;";
    memdb::core::QueryResult select_result = db.execute(select_query);
    ASSERT_TRUE(select_result.is_ok());
    ASSERT_EQ(select_result.get_data().size(), 3);

    EXPECT_EQ(select_result.get_data()[0][1]->get_string(), "Electronics");
    EXPECT_EQ(select_result.get_data()[1][1]->get_string(), "Electronics");
    EXPECT_EQ(select_result.get_data()[2][1]->get_string(), "Gadgets");
}

TEST(DeleteTest, DeleteWithIndexesAndWhereCondition) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_query = "create table inventory ({key, autoincrement} item_id : int32, name: string[30], category: string[20], stock: int32);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);

    std::string create_index = "create unordered index on inventory by category;";
    memdb::core::ParsedQuery pq_index = parser.parse(create_index);
    memdb::core::QueryResult index_result = db.execute(create_index);
    ASSERT_TRUE(index_result.is_ok());

    std::vector<std::string> insert_queries = {
        "insert (, \"Widget\", \"Tools\", 50) to inventory;",
        "insert (, \"Gadget\", \"Electronics\", 30) to inventory;",
        "insert (, \"Doohickey\", \"Tools\", 20) to inventory;",
        "insert (, \"Thingamabob\", \"Electronics\", 60) to inventory;",
        "insert (, \"Whatsit\", \"Gadgets\", 40) to inventory;"
    };
    for (const auto& query : insert_queries) {
        memdb::core::ParsedQuery pq_insert = parser.parse(query);
        db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
    }

    std::string delete_query = "delete inventory where category = \"Electronics\" && stock < 50;";
    memdb::core::QueryResult delete_result = db.execute(delete_query);
    ASSERT_TRUE(delete_result.is_ok());
    ASSERT_EQ(delete_result.get_data().size(), 1);
    EXPECT_EQ(delete_result.get_data()[0][0]->get_int(), 1);

    std::string select_query = "select name, category, stock from inventory;";
    memdb::core::QueryResult select_result = db.execute(select_query);
    ASSERT_TRUE(select_result.is_ok());
    ASSERT_EQ(select_result.get_data().size(), 4);

    EXPECT_EQ(select_result.get_data()[0][0]->get_string(), "Widget");
    EXPECT_EQ(select_result.get_data()[1][0]->get_string(), "Doohickey");
    EXPECT_EQ(select_result.get_data()[2][0]->get_string(), "Thingamabob");
    EXPECT_EQ(select_result.get_data()[3][0]->get_string(), "Whatsit");
}

TEST(DeleteTest, DeleteWithDefaultValueOverwrite) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_query = "create table devices ({key, autoincrement} device_id : int32, type: string[20], active: bool = true);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);

    std::string insert_query1 = "insert (, \"Router\") to devices;";
    std::string insert_query2 = "insert (, \"Switch\", false) to devices;";
    ASSERT_NO_THROW({
        memdb::core::ParsedQuery pq_insert1 = parser.parse(insert_query1);
        db.insert_row(pq_insert1.table_name, *pq_insert1.insert_values);

        memdb::core::ParsedQuery pq_insert2 = parser.parse(insert_query2);
        db.insert_row(pq_insert2.table_name, *pq_insert2.insert_values);
    });

    std::string delete_query = "delete devices where active = true;";
    memdb::core::QueryResult delete_result = db.execute(delete_query);
    ASSERT_TRUE(delete_result.is_ok());
    ASSERT_EQ(delete_result.get_data().size(), 1);
    EXPECT_EQ(delete_result.get_data()[0][0]->get_int(), 1);

    std::string select_query = "select device_id, type, active from devices;";
    memdb::core::QueryResult select_result = db.execute(select_query);
    ASSERT_TRUE(select_result.is_ok());
    ASSERT_EQ(select_result.get_data().size(), 1);

    EXPECT_EQ(select_result.get_data()[0][1]->get_string(), "Switch");
    EXPECT_EQ(select_result.get_data()[0][2]->get_bool(), false);
}

TEST(DeleteTest, DeleteWithComplexNestedConditions) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_query = "create table records ({key, autoincrement} record_id : int32, value1 : int32, value2 : int32, flag : bool);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);

    std::vector<std::string> insert_queries = {
        "insert (, 10, 20, true) to records;",
        "insert (, 15, 25, false) to records;",
        "insert (, 20, 30, true) to records;",
        "insert (, 25, 35, false) to records;",
        "insert (, 30, 40, true) to records;"
    };
    for (const auto& query : insert_queries) {
        memdb::core::ParsedQuery pq_insert = parser.parse(query);
        db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
    }

    std::string delete_query = "delete records where (value1 + value2) > 40 && flag || !(value1 < 15);";
    memdb::core::QueryResult delete_result = db.execute(delete_query);
    ASSERT_TRUE(delete_result.is_ok());
    ASSERT_EQ(delete_result.get_data().size(), 1);
    EXPECT_EQ(delete_result.get_data()[0][0]->get_int(), 4);

    std::string select_query = "select record_id, value1, value2, flag from records;";
    memdb::core::QueryResult select_result = db.execute(select_query);
    ASSERT_TRUE(select_result.is_ok());
    ASSERT_EQ(select_result.get_data().size(), 1);

    EXPECT_EQ(select_result.get_data()[0][1]->get_int(), 10);
    EXPECT_EQ(select_result.get_data()[0][2]->get_int(), 20);
    EXPECT_EQ(select_result.get_data()[0][3]->get_bool(), true);
}

TEST(DeleteTest, DeleteWithConstraintViolationsInPostDelete) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_customers = "create table customers ({key} customer_id : int32, name: string[30]);";
    std::string create_orders = "create table orders ({key} order_id : int32, customer_id : int32, amount : int32);";
    ASSERT_NO_THROW({
        memdb::core::ParsedQuery pq_customers = parser.parse(create_customers);
        db.create_table(pq_customers.table_name, pq_customers.columns);

        memdb::core::ParsedQuery pq_orders = parser.parse(create_orders);
        db.create_table(pq_orders.table_name, pq_orders.columns);
    });

    std::vector<std::string> insert_customers = {
        "insert (1, \"Alice\") to customers;",
        "insert (2, \"Bob\") to customers;"
    };
    std::vector<std::string> insert_orders = {
        "insert (101, 1, 250) to orders;",
        "insert (102, 2, 450) to orders;",
        "insert (103, 1, 150) to orders;"
    };

    for (const auto& query : insert_customers) {
        memdb::core::ParsedQuery pq = parser.parse(query);
        db.insert_row(pq.table_name, *pq.insert_values);
    }

    for (const auto& query : insert_orders) {
        memdb::core::ParsedQuery pq = parser.parse(query);
        db.insert_row(pq.table_name, *pq.insert_values);
    }

    std::string delete_query = "delete customers where name = \"Alice\";";
    memdb::core::QueryResult delete_result = db.execute(delete_query);
    ASSERT_TRUE(delete_result.is_ok());
    ASSERT_EQ(delete_result.get_data().size(), 1);
    EXPECT_EQ(delete_result.get_data()[0][0]->get_int(), 1);

    std::string select_orders_query = "select order_id, customer_id, amount from orders;";
    memdb::core::QueryResult select_orders_result = db.execute(select_orders_query);
    ASSERT_TRUE(select_orders_result.is_ok());
    ASSERT_EQ(select_orders_result.get_data().size(), 3);
    std::vector<std::tuple<int, int, int>> expected_orders = {
        {101, 1, 250},
        {102, 2, 450},
        {103, 1, 150}
    };

    for (const auto& expected : expected_orders) {
        bool found = false;
        for (const auto& row : select_orders_result.get_data()) {
            if (row.size() < 3) continue;
            if (row[0]->get_int() == std::get<0>(expected) &&
                row[1]->get_int() == std::get<1>(expected) &&
                row[2]->get_int() == std::get<2>(expected)) {
                found = true;
                break;
            }
        }
        EXPECT_TRUE(found) << "Order not found: "
                           << "order_id=" << std::get<0>(expected) << ", "
                           << "customer_id=" << std::get<1>(expected) << ", "
                           << "amount=" << std::get<2>(expected);
    }
}