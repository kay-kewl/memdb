#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "memdb/core/Database.h"
#include "memdb/core/QueryParser.h"

TEST(SelectTest, SelectAllColumns) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_query = "create table users ({key, autoincrement} id : int32, name: string[32], age: int32, is_admin: bool = false);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);

    std::string insert_query1 = "insert (1, \"Alice\", 30, true) to users;";
    std::string insert_query2 = "insert (2, \"Bob\", 25) to users;";
    ASSERT_NO_THROW({
        memdb::core::ParsedQuery pq_insert1 = parser.parse(insert_query1);
        db.insert_row(pq_insert1.table_name, *pq_insert1.insert_values);

        memdb::core::ParsedQuery pq_insert2 = parser.parse(insert_query2);
        db.insert_row(pq_insert2.table_name, *pq_insert2.insert_values);
    });

    std::string select_query = "select id, name, age, is_admin from users;";
    memdb::core::QueryResult result = db.execute(select_query);

    ASSERT_TRUE(result.is_ok());
    ASSERT_EQ(result.get_data().size(), 2);
    EXPECT_EQ(result.get_data()[0][0]->get_int(), 1);
    EXPECT_EQ(result.get_data()[0][1]->get_string(), "Alice");
    EXPECT_EQ(result.get_data()[0][2]->get_int(), 30);
    EXPECT_EQ(result.get_data()[0][3]->get_bool(), true);
    EXPECT_EQ(result.get_data()[1][0]->get_int(), 2);
    EXPECT_EQ(result.get_data()[1][1]->get_string(), "Bob");
    EXPECT_EQ(result.get_data()[1][2]->get_int(), 25);
    EXPECT_EQ(result.get_data()[1][3]->get_bool(), false);
}

TEST(SelectTest, SelectSpecificColumns) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_query = "create table employees ({key} emp_id : int32, first_name: string[20], last_name: string[20], salary: int32, department: string[30]);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);

    std::vector<std::string> insert_queries = {
        "insert (101, \"John\", \"Doe\", 50000, \"Engineering\") to employees;",
        "insert (102, \"Jane\", \"Smith\", 60000, \"Marketing\") to employees;",
        "insert (103, \"Emily\", \"Jones\", 55000, \"Engineering\") to employees;"
    };
    for (const auto& query : insert_queries) {
        ASSERT_NO_THROW({
            memdb::core::ParsedQuery pq_insert = parser.parse(query);
            db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
        });
    }

    std::string select_query = "select first_name, department from employees;";
    memdb::core::QueryResult result = db.execute(select_query);

    ASSERT_TRUE(result.is_ok());
    ASSERT_EQ(result.get_data().size(), 3);
    EXPECT_EQ(result.get_data()[0][0]->get_string(), "John");
    EXPECT_EQ(result.get_data()[0][1]->get_string(), "Engineering");
    EXPECT_EQ(result.get_data()[1][0]->get_string(), "Jane");
    EXPECT_EQ(result.get_data()[1][1]->get_string(), "Marketing");
    EXPECT_EQ(result.get_data()[2][0]->get_string(), "Emily");
    EXPECT_EQ(result.get_data()[2][1]->get_string(), "Engineering");
}

TEST(SelectTest, SelectWithWhereConditionEquality) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_query = "create table products ({key} product_id : int32, name: string[50], price: int32, in_stock: bool = true);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);

    std::vector<std::string> insert_queries = {
        "insert (1, \"Laptop\", 1500, true) to products;",
        "insert (2, \"Smartphone\", 800) to products;",
        "insert (3, \"Tablet\", 600, false) to products;"
    };
    for (const auto& query : insert_queries) {
        ASSERT_NO_THROW({
            memdb::core::ParsedQuery pq_insert = parser.parse(query);
            db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
        });
    }

    std::string select_query = "select product_id, name, price, in_stock from products where price = 800;";
    memdb::core::QueryResult result = db.execute(select_query);

    ASSERT_TRUE(result.is_ok());
    ASSERT_EQ(result.get_data().size(), 1);
    EXPECT_EQ(result.get_data()[0][0]->get_int(), 2);
    EXPECT_EQ(result.get_data()[0][1]->get_string(), "Smartphone");
    EXPECT_EQ(result.get_data()[0][2]->get_int(), 800);
    EXPECT_EQ(result.get_data()[0][3]->get_bool(), true); 
}

TEST(SelectTest, SelectWithWhereConditionLogicalOperators) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_query = "create table orders ({key} order_id : int32, customer: string[30], amount: int32, status: string[20]);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);

    std::vector<std::string> insert_queries = {
        "insert (1001, \"Alice\", 250, \"shipped\") to orders;",
        "insert (1002, \"Bob\", 450, \"processing\") to orders;",
        "insert (1003, \"Charlie\", 150, \"shipped\") to orders;",
        "insert (1004, \"Diana\", 500, \"processing\") to orders;",
        "insert (1005, \"Eve\", 300, \"shipped\") to orders;"
    };
    for (const auto& query : insert_queries) {
        ASSERT_NO_THROW({
            memdb::core::ParsedQuery pq_insert = parser.parse(query);
            db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
        });
    }

    std::string select_query = "select order_id, customer, amount, status from orders where amount > 200 && status = \"shipped\";";
    memdb::core::QueryResult result = db.execute(select_query);

    ASSERT_TRUE(result.is_ok());
    ASSERT_EQ(result.get_data().size(), 2);
    EXPECT_EQ(result.get_data()[0][0]->get_int(), 1001);
    EXPECT_EQ(result.get_data()[0][1]->get_string(), "Alice");
    EXPECT_EQ(result.get_data()[0][2]->get_int(), 250);
    EXPECT_EQ(result.get_data()[0][3]->get_string(), "shipped");
    EXPECT_EQ(result.get_data()[1][0]->get_int(), 1005);
    EXPECT_EQ(result.get_data()[1][1]->get_string(), "Eve");
    EXPECT_EQ(result.get_data()[1][2]->get_int(), 300);
    EXPECT_EQ(result.get_data()[1][3]->get_string(), "shipped");
}

TEST(SelectTest, SelectLengthFunction) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_query = "create table books ({key} book_id : int32, title: string[100], author: string[50]);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);

    std::vector<std::string> insert_queries = {
        "insert (1, \"The Great Gatsby\", \"F. Scott Fitzgerald\") to books;",
        "insert (2, \"1984\", \"George Orwell\") to books;",
        "insert (3, \"To Kill a Mockingbird\", \"Harper Lee\") to books;"
    };
    for (const auto& query : insert_queries) {
        ASSERT_NO_THROW({
            memdb::core::ParsedQuery pq_insert = parser.parse(query);
            db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
        });
    }

    std::string select_query = "select book_id, title, |title| as title_length from books where |title| > 10;";
    memdb::core::QueryResult result = db.execute(select_query);

    ASSERT_TRUE(result.is_ok());
    ASSERT_EQ(result.get_data().size(), 2);
    EXPECT_EQ(result.get_data()[0][0]->get_int(), 1);
    EXPECT_EQ(result.get_data()[0][1]->get_string(), "The Great Gatsby");
    EXPECT_EQ(result.get_data()[0][2]->get_int(), 16);
    EXPECT_EQ(result.get_data()[1][0]->get_int(), 3);
    EXPECT_EQ(result.get_data()[1][1]->get_string(), "To Kill a Mockingbird");
    EXPECT_EQ(result.get_data()[1][2]->get_int(), 21);
}

TEST(SelectTest, SelectWithJoin) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_users = "create table users ({key, autoincrement} id : int32, name: string[30], department: string[20]);";
    ASSERT_NO_THROW({
        memdb::core::ParsedQuery pq_create = parser.parse(create_users);
        db.create_table(pq_create.table_name, pq_create.columns);
    });

    std::string create_projects = "create table projects ({key} project_id : int32, project_name: string[50], user_id: int32);";
    ASSERT_NO_THROW({
        memdb::core::ParsedQuery pq_create = parser.parse(create_projects);
        db.create_table(pq_create.table_name, pq_create.columns);
    });

    std::vector<std::string> insert_users = {
        "insert (, \"Alice\", \"Engineering\") to users;",
        "insert (, \"Bob\", \"Marketing\") to users;",
        "insert (, \"Charlie\", \"Engineering\") to users;"
    };
    for (const auto& query : insert_users) {
        ASSERT_NO_THROW({
            memdb::core::ParsedQuery pq_insert = parser.parse(query);
            db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
        });
    }

    std::vector<std::string> insert_projects = {
        "insert (101, \"Project Alpha\", 1) to projects;",
        "insert (102, \"Project Beta\", 2) to projects;",
        "insert (103, \"Project Gamma\", 1) to projects;",
        "insert (104, \"Project Delta\", 3) to projects;"
    };
    for (const auto& query : insert_projects) {
        ASSERT_NO_THROW({
            memdb::core::ParsedQuery pq_insert = parser.parse(query);
            db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
        });
    }

    std::string select_query = "select users.name, projects.project_name from users join projects on users.id = projects.user_id where users.department = \"Engineering\";";
    memdb::core::QueryResult result = db.execute(select_query);

    ASSERT_TRUE(result.is_ok());
    ASSERT_EQ(result.get_data().size(), 3);
    EXPECT_EQ(result.get_data()[0][0]->get_string(), "Alice");
    EXPECT_EQ(result.get_data()[0][1]->get_string(), "Project Alpha");
    EXPECT_EQ(result.get_data()[1][0]->get_string(), "Alice");
    EXPECT_EQ(result.get_data()[1][1]->get_string(), "Project Gamma");
    EXPECT_EQ(result.get_data()[2][0]->get_string(), "Charlie");
    EXPECT_EQ(result.get_data()[2][1]->get_string(), "Project Delta");
}

TEST(SelectTest, SelectFromEmptyTable) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_query = "create table empty_table (id : int32, description: string[50]);";
    ASSERT_NO_THROW({
        memdb::core::ParsedQuery pq_create = parser.parse(create_query);
        db.create_table(pq_create.table_name, pq_create.columns);
    });

    std::string select_query = "select id, description from empty_table;";
    memdb::core::QueryResult result = db.execute(select_query);

    ASSERT_TRUE(result.is_ok());
    ASSERT_EQ(result.get_data().size(), 0);
}

TEST(SelectTest, SelectWithNoMatchingRows) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_query = "create table inventory (item_id : int32, item_name: string[30], quantity: int32);";
    ASSERT_NO_THROW({
        memdb::core::ParsedQuery pq_create = parser.parse(create_query);
        db.create_table(pq_create.table_name, pq_create.columns);
    });

    std::vector<std::string> insert_queries = {
        "insert (1, \"Widget\", 100) to inventory;",
        "insert (2, \"Gadget\", 50) to inventory;",
        "insert (3, \"Thingamajig\", 0) to inventory;"
    };
    for (const auto& query : insert_queries) {
        ASSERT_NO_THROW({
            memdb::core::ParsedQuery pq_insert = parser.parse(query);
            db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
        });
    }

    std::string select_query = "select item_id, item_name, quantity from inventory where quantity > 200;";
    memdb::core::QueryResult result = db.execute(select_query);

    ASSERT_TRUE(result.is_ok());
    ASSERT_EQ(result.get_data().size(), 0);
}

TEST(SelectTest, SelectWithNotOperator) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_query = "create table devices ({key} device_id : int32, device_name: string[30], active: bool);";
    ASSERT_NO_THROW({
        memdb::core::ParsedQuery pq_create = parser.parse(create_query);
        db.create_table(pq_create.table_name, pq_create.columns);
    });

    std::vector<std::string> insert_queries = {
        "insert (1, \"Router\", true) to devices;",
        "insert (2, \"Switch\", false) to devices;",
        "insert (3, \"Firewall\", true) to devices;",
        "insert (4, \"Access Point\", false) to devices;"
    };
    for (const auto& query : insert_queries) {
        ASSERT_NO_THROW({
            memdb::core::ParsedQuery pq_insert = parser.parse(query);
            db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
        });
    }

    std::string select_query = "select device_id, device_name from devices where !active;";
    memdb::core::QueryResult result = db.execute(select_query);

    ASSERT_TRUE(result.is_ok());
    ASSERT_EQ(result.get_data().size(), 2);
    EXPECT_EQ(result.get_data()[0][0]->get_int(), 2);
    EXPECT_EQ(result.get_data()[0][1]->get_string(), "Switch");
    EXPECT_EQ(result.get_data()[1][0]->get_int(), 4);
    EXPECT_EQ(result.get_data()[1][1]->get_string(), "Access Point");
}

TEST(SelectTest, SelectWithMultipleLogicalOperators) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_query = "create table sales ({key} sale_id : int32, product: string[30], quantity: int32, region: string[20]);";
    ASSERT_NO_THROW({
        memdb::core::ParsedQuery pq_create = parser.parse(create_query);
        db.create_table(pq_create.table_name, pq_create.columns);
    });

    std::vector<std::string> insert_queries = {
        "insert (1, \"Laptop\", 5, \"North\") to sales;",
        "insert (2, \"Laptop\", 10, \"South\") to sales;",
        "insert (3, \"Smartphone\", 15, \"North\") to sales;",
        "insert (4, \"Tablet\", 20, \"East\") to sales;",
        "insert (5, \"Laptop\", 25, \"West\") to sales;"
    };
    for (const auto& query : insert_queries) {
        ASSERT_NO_THROW({
            memdb::core::ParsedQuery pq_insert = parser.parse(query);
            db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
        });
    }

    std::string select_query = "select sale_id, product, quantity, region from sales where (product = \"Laptop\" && quantity >= 10) || region = \"East\";";
    memdb::core::QueryResult result = db.execute(select_query);

    ASSERT_TRUE(result.is_ok());
    ASSERT_EQ(result.get_data().size(), 3);
    EXPECT_EQ(result.get_data()[0][0]->get_int(), 2);
    EXPECT_EQ(result.get_data()[0][1]->get_string(), "Laptop");
    EXPECT_EQ(result.get_data()[0][2]->get_int(), 10);
    EXPECT_EQ(result.get_data()[0][3]->get_string(), "South");
    EXPECT_EQ(result.get_data()[1][0]->get_int(), 4);
    EXPECT_EQ(result.get_data()[1][1]->get_string(), "Tablet");
    EXPECT_EQ(result.get_data()[1][2]->get_int(), 20);
    EXPECT_EQ(result.get_data()[1][3]->get_string(), "East");
    EXPECT_EQ(result.get_data()[2][0]->get_int(), 5);
    EXPECT_EQ(result.get_data()[2][1]->get_string(), "Laptop");
    EXPECT_EQ(result.get_data()[2][2]->get_int(), 25);
    EXPECT_EQ(result.get_data()[2][3]->get_string(), "West");
}

TEST(SelectTest, SelectWithLengthFunction) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_query = "create table books ({key} book_id : int32, title: string[100], author: string[50]);";
    ASSERT_NO_THROW({
        memdb::core::ParsedQuery pq_create = parser.parse(create_query);
        db.create_table(pq_create.table_name, pq_create.columns);
    });

    std::vector<std::string> insert_queries = {
        "insert (1, \"The Great Gatsby\", \"F. Scott Fitzgerald\") to books;",
        "insert (2, \"1984\", \"George Orwell\") to books;",
        "insert (3, \"To Kill a Mockingbird\", \"Harper Lee\") to books;"
    };
    for (const auto& query : insert_queries) {
        ASSERT_NO_THROW({
            memdb::core::ParsedQuery pq_insert = parser.parse(query);
            db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
        });
    }

    std::string select_query = "select book_id, title, |title| as title_length from books where |title| > 10;";
    memdb::core::QueryResult result = db.execute(select_query);

    ASSERT_TRUE(result.is_ok());
    ASSERT_EQ(result.get_data().size(), 2);
    EXPECT_EQ(result.get_data()[0][0]->get_int(), 1);
    EXPECT_EQ(result.get_data()[0][1]->get_string(), "The Great Gatsby");
    EXPECT_EQ(result.get_data()[0][2]->get_int(), 16);
    EXPECT_EQ(result.get_data()[1][0]->get_int(), 3);
    EXPECT_EQ(result.get_data()[1][1]->get_string(), "To Kill a Mockingbird");
    EXPECT_EQ(result.get_data()[1][2]->get_int(), 21);
}

TEST(SelectTest, SelectWithJoinAndWhere) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_customers = "create table customers ({key, autoincrement} customer_id : int32, name: string[30], city: string[20]);";
    ASSERT_NO_THROW({
        memdb::core::ParsedQuery pq_create = parser.parse(create_customers);
        db.create_table(pq_create.table_name, pq_create.columns);
    });

    std::string create_orders = "create table orders ({key} order_id : int32, customer_id: int32, amount: int32);";
    ASSERT_NO_THROW({
        memdb::core::ParsedQuery pq_create = parser.parse(create_orders);
        db.create_table(pq_create.table_name, pq_create.columns);
    });

    std::vector<std::string> insert_customers = {
        "insert (, \"Alice\", \"New York\") to customers;",
        "insert (, \"Bob\", \"Los Angeles\") to customers;",
        "insert (, \"Charlie\", \"Chicago\") to customers;"
    };
    for (const auto& query : insert_customers) {
        ASSERT_NO_THROW({
            memdb::core::ParsedQuery pq_insert = parser.parse(query);
            db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
        });
    }

    std::vector<std::string> insert_orders = {
        "insert (101, 1, 300) to orders;",
        "insert (102, 2, 200) to orders;",
        "insert (103, 1, 450) to orders;",
        "insert (104, 3, 500) to orders;"
    };
    for (const auto& query : insert_orders) {
        ASSERT_NO_THROW({
            memdb::core::ParsedQuery pq_insert = parser.parse(query);
            db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
        });
    }

    std::string select_query = "select customers.name, orders.order_id, orders.amount from customers join orders on customers.customer_id = orders.customer_id where orders.amount > 300;";
    memdb::core::QueryResult result = db.execute(select_query);

    ASSERT_TRUE(result.is_ok());
    ASSERT_EQ(result.get_data().size(), 2);
    ASSERT_EQ(result.get_data()[0][0]->get_string(), "Alice");
    ASSERT_EQ(result.get_data()[0][1]->get_int(), 103);
    ASSERT_EQ(result.get_data()[0][2]->get_int(), 450);
    EXPECT_EQ(result.get_data()[1][0]->get_string(), "Charlie");
    EXPECT_EQ(result.get_data()[1][1]->get_int(), 104);
    EXPECT_EQ(result.get_data()[1][2]->get_int(), 500);
}

TEST(SelectTest, SelectFromEmptyJoinResult) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_customers = "create table customers ({key, autoincrement} customer_id : int32, name: string[30], city: string[20]);";
    ASSERT_NO_THROW({
        memdb::core::ParsedQuery pq_create = parser.parse(create_customers);
        db.create_table(pq_create.table_name, pq_create.columns);
    });

    std::string create_orders = "create table orders ({key} order_id : int32, customer_id: int32, amount: int32);";
    ASSERT_NO_THROW({
        memdb::core::ParsedQuery pq_create = parser.parse(create_orders);
        db.create_table(pq_create.table_name, pq_create.columns);
    });

    std::vector<std::string> insert_customers = {
        "insert (1, \"Alice\", \"New York\") to customers;",
        "insert (2, \"Bob\", \"Los Angeles\") to customers;"
    };
    for (const auto& query : insert_customers) {
        ASSERT_NO_THROW({
            memdb::core::ParsedQuery pq_insert = parser.parse(query);
            db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
        });
    }

    std::vector<std::string> insert_orders = {
        "insert (101, 3, 300) to orders;",
        "insert (102, 4, 200) to orders;"
    };
    for (const auto& query : insert_orders) {
        ASSERT_NO_THROW({
            memdb::core::ParsedQuery pq_insert = parser.parse(query);
            db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
        });
    }

    std::string select_query = "select customers.name, orders.order_id, orders.amount from customers join orders on customers.customer_id = orders.customer_id where customers.city = \"Chicago\";";
    memdb::core::QueryResult result = db.execute(select_query);

    ASSERT_TRUE(result.is_ok());
    ASSERT_EQ(result.get_data().size(), 0);
}

TEST(SelectTest, SelectWithAggregateFunctions) {
}

TEST(SelectTest, SelectFromNonExistentTable) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string select_query = "select id, name from nonexistent_table;";
    memdb::core::QueryResult result = db.execute(select_query);

    ASSERT_FALSE(result.is_ok());
    EXPECT_THAT(result.get_error(), ::testing::HasSubstr("Table not found: nonexistent_table"));
}

TEST(SelectTest, SelectFromNonExistentColumn) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_query = "create table inventory (item_id : int32, item_name: string[30], quantity: int32);";
    ASSERT_NO_THROW({
        memdb::core::ParsedQuery pq_create = parser.parse(create_query);
        db.create_table(pq_create.table_name, pq_create.columns);
    });

    std::string insert_query = "insert (1, \"Widget\", 100) to inventory;";
    ASSERT_NO_THROW({
        memdb::core::ParsedQuery pq_insert = parser.parse(insert_query);
        db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
    });

    std::string select_query = "select item_id, price from inventory;";
    memdb::core::QueryResult result = db.execute(select_query);

    ASSERT_FALSE(result.is_ok());
    EXPECT_THAT(result.get_error(), ::testing::HasSubstr("Column not found: price"));
}

TEST(SelectTest, SelectWithExpressionsInColumns) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_query = "create table accounts ({key} account_id : int32, balance: int32);";
    ASSERT_NO_THROW({
        memdb::core::ParsedQuery pq_create = parser.parse(create_query);
        db.create_table(pq_create.table_name, pq_create.columns);
    });

    std::vector<std::string> insert_queries = {
        "insert (1, 1000) to accounts;",
        "insert (2, 2000) to accounts;",
        "insert (3, 3000) to accounts;"
    };
    for (const auto& query : insert_queries) {
        ASSERT_NO_THROW({
            memdb::core::ParsedQuery pq_insert = parser.parse(query);
            db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
        });
    }

    std::string select_query = "select account_id, balance, balance * 2 as double_balance from accounts;";
    memdb::core::QueryResult result = db.execute(select_query);

    ASSERT_TRUE(result.is_ok());
    ASSERT_EQ(result.get_data().size(), 3);

    EXPECT_EQ(result.get_data()[0][0]->get_int(), 1);
    EXPECT_EQ(result.get_data()[0][1]->get_int(), 1000);
    EXPECT_EQ(result.get_data()[0][2]->get_int(), 2000);

    EXPECT_EQ(result.get_data()[1][0]->get_int(), 2);
    EXPECT_EQ(result.get_data()[1][1]->get_int(), 2000);
    EXPECT_EQ(result.get_data()[1][2]->get_int(), 4000);

    EXPECT_EQ(result.get_data()[2][0]->get_int(), 3);
    EXPECT_EQ(result.get_data()[2][1]->get_int(), 3000);
    EXPECT_EQ(result.get_data()[2][2]->get_int(), 6000);
}

TEST(SelectTest, SelectWithJoinAndNoMatchingRows) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_authors = "create table authors ({key, autoincrement} author_id : int32, name: string[30]);";
    ASSERT_NO_THROW({
        memdb::core::ParsedQuery pq_create = parser.parse(create_authors);
        db.create_table(pq_create.table_name, pq_create.columns);
    });

    std::string create_books = "create table books ({key} book_id : int32, title: string[50], author_id: int32);";
    ASSERT_NO_THROW({
        memdb::core::ParsedQuery pq_create = parser.parse(create_books);
        db.create_table(pq_create.table_name, pq_create.columns);
    });

    std::vector<std::string> insert_authors = {
        "insert (, \"George Orwell\") to authors;",
        "insert (, \"J.K. Rowling\") to authors;"
    };
    for (const auto& query : insert_authors) {
        ASSERT_NO_THROW({
            memdb::core::ParsedQuery pq_insert = parser.parse(query);
            db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
        });
    }

    std::vector<std::string> insert_books = {
        "insert (101, \"1984\", 1) to books;",
        "insert (102, \"Harry Potter\", 2) to books;",
        "insert (103, \"Animal Farm\", 1) to books;"
    };
    for (const auto& query : insert_books) {
        ASSERT_NO_THROW({
            memdb::core::ParsedQuery pq_insert = parser.parse(query);
            db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
        });
    }

    std::string select_query = "select authors.name, books.title from authors join books on authors.author_id = books.author_id where authors.name = \"Unknown\";";
    memdb::core::QueryResult result = db.execute(select_query);

    ASSERT_TRUE(result.is_ok());
    ASSERT_EQ(result.get_data().size(), 0);
}

TEST(SelectTest, SelectWithInvalidSelectStatement) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_query = "create table inventory (item_id : int32, item_name: string[30], quantity: int32);";
    ASSERT_NO_THROW({
        memdb::core::ParsedQuery pq_create = parser.parse(create_query);
        db.create_table(pq_create.table_name, pq_create.columns);
    });

    std::string insert_query = "insert (1, \"Widget\", 100) to inventory;";
    ASSERT_NO_THROW({
        memdb::core::ParsedQuery pq_insert = parser.parse(insert_query);
        db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
    });

    std::string select_query = "select item_id, price from inventory;";
    memdb::core::QueryResult result = db.execute(select_query);

    ASSERT_FALSE(result.is_ok());
    EXPECT_THAT(result.get_error(), ::testing::HasSubstr("Column not found: price"));
}

TEST(SelectTest, SelectWithExpressionInColumns) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_query = "create table metrics ({key} metric_id : int32, value1: int32, value2: int32);";
    ASSERT_NO_THROW({
        memdb::core::ParsedQuery pq_create = parser.parse(create_query);
        db.create_table(pq_create.table_name, pq_create.columns);
    });

    std::string insert_query1 = "insert (1, 10, 20) to metrics;";
    std::string insert_query2 = "insert (2, 15, 25) to metrics;";
    std::string insert_query3 = "insert (3, 20, 30) to metrics;";
    ASSERT_NO_THROW({
        memdb::core::ParsedQuery pq_insert1 = parser.parse(insert_query1);
        db.insert_row(pq_insert1.table_name, *pq_insert1.insert_values);

        memdb::core::ParsedQuery pq_insert2 = parser.parse(insert_query2);
        db.insert_row(pq_insert2.table_name, *pq_insert2.insert_values);

        memdb::core::ParsedQuery pq_insert3 = parser.parse(insert_query3);
        db.insert_row(pq_insert3.table_name, *pq_insert3.insert_values);
    });

    std::string select_query = "select metric_id, value1, value2, value1 + value2 as total from metrics;";
    memdb::core::QueryResult result = db.execute(select_query);

    ASSERT_TRUE(result.is_ok());
    ASSERT_EQ(result.get_data().size(), 3);

    EXPECT_EQ(result.get_data()[0][0]->get_int(), 1);
    EXPECT_EQ(result.get_data()[0][1]->get_int(), 10);
    EXPECT_EQ(result.get_data()[0][2]->get_int(), 20);
    EXPECT_EQ(result.get_data()[0][3]->get_int(), 30);

    EXPECT_EQ(result.get_data()[1][0]->get_int(), 2);
    EXPECT_EQ(result.get_data()[1][1]->get_int(), 15);
    EXPECT_EQ(result.get_data()[1][2]->get_int(), 25);
    EXPECT_EQ(result.get_data()[1][3]->get_int(), 40);

    EXPECT_EQ(result.get_data()[2][0]->get_int(), 3);
    EXPECT_EQ(result.get_data()[2][1]->get_int(), 20);
    EXPECT_EQ(result.get_data()[2][2]->get_int(), 30);
    EXPECT_EQ(result.get_data()[2][3]->get_int(), 50);
}

TEST(SelectTest, SelectWithInvalidDataTypesInExpressions) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_query = "create table types ({key} type_id : int32, name: string[30], flag: bool);";
    ASSERT_NO_THROW({
        memdb::core::ParsedQuery pq_create = parser.parse(create_query);
        db.create_table(pq_create.table_name, pq_create.columns);
    });

    std::string insert_query1 = "insert (1, \"TypeA\", true) to types;";
    std::string insert_query2 = "insert (2, \"TypeB\", false) to types;";
    ASSERT_NO_THROW({
        memdb::core::ParsedQuery pq_insert1 = parser.parse(insert_query1);
        db.insert_row(pq_insert1.table_name, *pq_insert1.insert_values);

        memdb::core::ParsedQuery pq_insert2 = parser.parse(insert_query2);
        db.insert_row(pq_insert2.table_name, *pq_insert2.insert_values);
    });

    std::string select_query = "select type_id, name, flag, name + flag as invalid_expression from types;";
    memdb::core::QueryResult result = db.execute(select_query);

    ASSERT_FALSE(result.is_ok());
    EXPECT_THAT(result.get_error(), ::testing::HasSubstr("Operator '+' not supported for given types."));
}

TEST(SelectTest, SelectWithJoinAndMultipleConditions) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_query = "create table departments ({key} dept_id : int32, dept_name: string[30]);";
    ASSERT_NO_THROW({
        memdb::core::ParsedQuery pq_create = parser.parse(create_query);
        db.create_table(pq_create.table_name, pq_create.columns);
    });

    std::string create_employees = "create table employees ({key, autoincrement} emp_id : int32, name: string[30], salary: int32, dept_id: int32, active: bool = true);";
    ASSERT_NO_THROW({
        memdb::core::ParsedQuery pq_create = parser.parse(create_employees);
        db.create_table(pq_create.table_name, pq_create.columns);
    });

    std::vector<std::string> insert_departments = {
        "insert (1, \"Engineering\") to departments;",
        "insert (2, \"Marketing\") to departments;",
        "insert (3, \"Sales\") to departments;"
    };
    for (const auto& query : insert_departments) {
        ASSERT_NO_THROW({
            memdb::core::ParsedQuery pq_insert = parser.parse(query);
            db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
        });
    }

    std::vector<std::string> insert_employees = {
        "insert (, \"Alice\", 70000, 1, true) to employees;",
        "insert (, \"Bob\", 50000, 1) to employees;",
        "insert (, \"Charlie\", 60000, 2, false) to employees;",
        "insert (, \"David\", 55000, 1) to employees;"
    };
    for (const auto& query : insert_employees) {
        ASSERT_NO_THROW({
            memdb::core::ParsedQuery pq_insert = parser.parse(query);
            db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
        });
    }

    auto employees_table = db.get_table("employees");
    ASSERT_EQ(employees_table->get_all_rows().size(), 4);

    std::string select_query = "select employees.emp_id, employees.name, departments.dept_name, employees.salary from employees join departments on employees.dept_id = departments.dept_id where departments.dept_name = \"Engineering\" && employees.salary > 60000 || employees.active = false;";
    memdb::core::QueryResult result = db.execute(select_query);

    ASSERT_TRUE(result.is_ok());

    ASSERT_EQ(result.get_data().size(), 2);

    EXPECT_EQ(result.get_data()[0][0]->get_int(), 1);
    EXPECT_EQ(result.get_data()[0][1]->get_string(), "Alice");
    EXPECT_EQ(result.get_data()[0][2]->get_string(), "Engineering");
    EXPECT_EQ(result.get_data()[0][3]->get_int(), 70000);

    EXPECT_EQ(result.get_data()[1][0]->get_int(), 3);
    EXPECT_EQ(result.get_data()[1][1]->get_string(), "Charlie");
    EXPECT_EQ(result.get_data()[1][2]->get_string(), "Marketing");
    EXPECT_EQ(result.get_data()[1][3]->get_int(), 60000);
}

TEST(SelectTest, SelectWithAliasedColumnsAndExpressions) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_query = "create table scores ({key} student_id : int32, name: string[30], math: int32, physics: int32);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);

    std::vector<std::string> insert_queries = {
        "insert (1, \"Alice\", 85, 90) to scores;",
        "insert (2, \"Bob\", 75, 80) to scores;",
        "insert (3, \"Charlie\", 95, 85) to scores;"
    };
    for (const auto& query : insert_queries) {
        ASSERT_NO_THROW({
            memdb::core::ParsedQuery pq_insert = parser.parse(query);
            db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
        });
    }

    std::string select_query = "select student_id as id, name, math + physics as total_score from scores where math + physics >= 170;";
    memdb::core::QueryResult result = db.execute(select_query);

    if (!result.is_ok()) {
        std::cerr << "Query Error: " << result.get_error() << std::endl;
    }

    ASSERT_TRUE(result.is_ok());

    ASSERT_EQ(result.get_data().size(), 2);

    EXPECT_EQ(result.get_data()[0][0]->get_int(), 1);
    EXPECT_EQ(result.get_data()[0][1]->get_string(), "Alice");
    EXPECT_EQ(result.get_data()[0][2]->get_int(), 175);

    EXPECT_EQ(result.get_data()[1][0]->get_int(), 3);
    EXPECT_EQ(result.get_data()[1][1]->get_string(), "Charlie");
    EXPECT_EQ(result.get_data()[1][2]->get_int(), 180);
}

TEST(SelectTest, ArithmeticOperations) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_query = "create table arithmetic (a : int32, b : int32);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);

    std::string insert_query = "insert (10, 5) to arithmetic;";
    memdb::core::ParsedQuery pq_insert = parser.parse(insert_query);
    db.insert_row(pq_insert.table_name, *pq_insert.insert_values);

    std::string select_add = "select a + b as sum from arithmetic;";
    memdb::core::QueryResult result_add = db.execute(select_add);
    ASSERT_TRUE(result_add.is_ok());
    EXPECT_EQ(result_add.get_data()[0][0]->get_int(), 15);

    std::string select_sub = "select a - b as difference from arithmetic;";
    memdb::core::QueryResult result_sub = db.execute(select_sub);
    ASSERT_TRUE(result_sub.is_ok());
    EXPECT_EQ(result_sub.get_data()[0][0]->get_int(), 5);

    std::string select_mul = "select a * b as product from arithmetic;";
    memdb::core::QueryResult result_mul = db.execute(select_mul);
    ASSERT_TRUE(result_mul.is_ok());
    EXPECT_EQ(result_mul.get_data()[0][0]->get_int(), 50);

    std::string select_div = "select a / b as quotient from arithmetic;";
    memdb::core::QueryResult result_div = db.execute(select_div);
    ASSERT_TRUE(result_div.is_ok());
    EXPECT_EQ(result_div.get_data()[0][0]->get_int(), 2);

    std::string select_mod = "select a % b as remainder from arithmetic;";
    memdb::core::QueryResult result_mod = db.execute(select_mod);
    ASSERT_TRUE(result_mod.is_ok());
    EXPECT_EQ(result_mod.get_data()[0][0]->get_int(), 0);
}

TEST(SelectTest, ComparisonOperations) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_query = "create table comparisons (num : int32, text : string[10], flag : bool);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);

    std::vector<std::string> insert_queries = {
        "insert (10, \"alpha\", true) to comparisons;",
        "insert (20, \"beta\", false) to comparisons;",
        "insert (15, \"gamma\", true) to comparisons;"
    };
    for (const auto& query : insert_queries) {
        memdb::core::ParsedQuery pq_insert = parser.parse(query);
        db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
    }

    std::string select_less = "select num from comparisons where num < 15;";
    memdb::core::QueryResult result_less = db.execute(select_less);
    ASSERT_TRUE(result_less.is_ok());
    ASSERT_EQ(result_less.get_data().size(), 1);
    EXPECT_EQ(result_less.get_data()[0][0]->get_int(), 10);

    std::string select_greater = "select num from comparisons where num > 15;";
    memdb::core::QueryResult result_greater = db.execute(select_greater);
    ASSERT_TRUE(result_greater.is_ok());
    ASSERT_EQ(result_greater.get_data().size(), 1);
    EXPECT_EQ(result_greater.get_data()[0][0]->get_int(), 20);

    std::string select_less_eq = "select num from comparisons where num <= 15;";
    memdb::core::QueryResult result_less_eq = db.execute(select_less_eq);
    ASSERT_TRUE(result_less_eq.is_ok());
    ASSERT_EQ(result_less_eq.get_data().size(), 2);
    EXPECT_EQ(result_less_eq.get_data()[0][0]->get_int(), 10);
    EXPECT_EQ(result_less_eq.get_data()[1][0]->get_int(), 15);

    std::string select_greater_eq = "select num from comparisons where num >= 15;";
    memdb::core::QueryResult result_greater_eq = db.execute(select_greater_eq);
    ASSERT_TRUE(result_greater_eq.is_ok());
    ASSERT_EQ(result_greater_eq.get_data().size(), 2);
    EXPECT_EQ(result_greater_eq.get_data()[0][0]->get_int(), 20);
    EXPECT_EQ(result_greater_eq.get_data()[1][0]->get_int(), 15);

    std::string select_eq = "select text from comparisons where text = \"beta\";";
    memdb::core::QueryResult result_eq = db.execute(select_eq);
    ASSERT_TRUE(result_eq.is_ok());
    ASSERT_EQ(result_eq.get_data().size(), 1);
    EXPECT_EQ(result_eq.get_data()[0][0]->get_string(), "beta");

    std::string select_neq = "select flag from comparisons where flag != true;";
    memdb::core::QueryResult result_neq = db.execute(select_neq);
    ASSERT_TRUE(result_neq.is_ok());
    ASSERT_EQ(result_neq.get_data().size(), 1);
    EXPECT_EQ(result_neq.get_data()[0][0]->get_bool(), false);
}

TEST(SelectTest, ComparisonTypeMismatch) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_query = "create table mismatches (id : int32, description: string[20]);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);

    std::string insert_query = "insert (1, \"Test\") to mismatches;";
    memdb::core::ParsedQuery pq_insert = parser.parse(insert_query);
    db.insert_row(pq_insert.table_name, *pq_insert.insert_values);

    std::string select_query = "select id from mismatches where id = description;";
    memdb::core::QueryResult result = db.execute(select_query);

    ASSERT_FALSE(result.is_ok());
    EXPECT_THAT(result.get_error(), ::testing::HasSubstr("Equality comparison requires operands of the same type."));
}

TEST(SelectTest, LogicalOperations) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_query = "create table logic (a : bool, b : bool, c : bool);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);

    std::vector<std::string> insert_queries = {
        "insert (true, true, false) to logic;",
        "insert (true, false, true) to logic;",
        "insert (false, false, true) to logic;"
    };
    for (const auto& query : insert_queries) {
        memdb::core::ParsedQuery pq_insert = parser.parse(query);
        db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
    }

    std::string select_and = "select a && b as and_result from logic;";
    memdb::core::QueryResult result_and = db.execute(select_and);
    ASSERT_TRUE(result_and.is_ok());
    EXPECT_EQ(result_and.get_data()[0][0]->get_bool(), true);
    EXPECT_EQ(result_and.get_data()[1][0]->get_bool(), false);
    EXPECT_EQ(result_and.get_data()[2][0]->get_bool(), false);

    std::string select_or = "select a || b as or_result from logic;";
    memdb::core::QueryResult result_or = db.execute(select_or);
    ASSERT_TRUE(result_or.is_ok());
    EXPECT_EQ(result_or.get_data()[0][0]->get_bool(), true);
    EXPECT_EQ(result_or.get_data()[1][0]->get_bool(), true);
    EXPECT_EQ(result_or.get_data()[2][0]->get_bool(), false);

    std::string select_not = "select !a as not_a from logic;";
    memdb::core::QueryResult result_not = db.execute(select_not);
    ASSERT_TRUE(result_not.is_ok());
    EXPECT_EQ(result_not.get_data()[0][0]->get_bool(), false);
    EXPECT_EQ(result_not.get_data()[1][0]->get_bool(), false);
    EXPECT_EQ(result_not.get_data()[2][0]->get_bool(), true);

    std::string select_xor = "select a ^^ b as xor_result from logic;";
    memdb::core::QueryResult result_xor = db.execute(select_xor);
    ASSERT_TRUE(result_xor.is_ok());
    EXPECT_EQ(result_xor.get_data()[0][0]->get_bool(), false);
    EXPECT_EQ(result_xor.get_data()[1][0]->get_bool(), true);
    EXPECT_EQ(result_xor.get_data()[2][0]->get_bool(), false);
}

TEST(SelectTest, LogicalTypeMismatch) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_query = "create table invalid_logic (a : bool, b : int32);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);

    std::string insert_query = "insert (true, 10) to invalid_logic;";
    memdb::core::ParsedQuery pq_insert = parser.parse(insert_query);
    db.insert_row(pq_insert.table_name, *pq_insert.insert_values);

    std::string select_query = "select a && b as invalid from invalid_logic;";
    memdb::core::QueryResult result = db.execute(select_query);

    ASSERT_FALSE(result.is_ok());
    EXPECT_THAT(result.get_error(), ::testing::HasSubstr("Operator '&&' requires Bool types."));
}

TEST(SelectTest, LengthFunction) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_query = "create table strings (s : string[50], b : bytes[10]);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);

    std::vector<std::string> insert_queries = {
        "insert (\"hello\", 0x010203) to strings;",
        "insert (\"\", 0x00) to strings;",
        "insert (\"a longer string\", 0xFFEEDDCC) to strings;"
    };
    for (const auto& query : insert_queries) {
        memdb::core::ParsedQuery pq_insert = parser.parse(query);
        db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
    }

    std::string select_length_str = "select |s| as length_s from strings;";
    memdb::core::QueryResult result_length_str = db.execute(select_length_str);
    ASSERT_TRUE(result_length_str.is_ok());
    EXPECT_EQ(result_length_str.get_data()[0][0]->get_int(), 5);
    EXPECT_EQ(result_length_str.get_data()[1][0]->get_int(), 0);
    EXPECT_EQ(result_length_str.get_data()[2][0]->get_int(), 15);

    std::string select_length_bytes = "select |b| as length_b from strings;";
    memdb::core::QueryResult result_length_bytes = db.execute(select_length_bytes);
    ASSERT_TRUE(result_length_bytes.is_ok());
    EXPECT_EQ(result_length_bytes.get_data()[0][0]->get_int(), 3);
    EXPECT_EQ(result_length_bytes.get_data()[1][0]->get_int(), 1);
    EXPECT_EQ(result_length_bytes.get_data()[2][0]->get_int(), 4);
}

TEST(SelectTest, LengthFunctionInvalidType) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_query = "create table invalid_length (num : int32);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);

    std::string insert_query = "insert (10) to invalid_length;";
    memdb::core::ParsedQuery pq_insert = parser.parse(insert_query);
    db.insert_row(pq_insert.table_name, *pq_insert.insert_values);

    std::string select_query = "select |num| as length_num from invalid_length;";
    memdb::core::QueryResult result = db.execute(select_query);

    ASSERT_FALSE(result.is_ok());
    EXPECT_THAT(result.get_error(), ::testing::HasSubstr("Operator '|var|' requires String or Bytes type."));
}

TEST(SelectTest, StringConcatenation) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_query = "create table greetings (first : string[10], second : string[10]);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);

    std::string insert_query = "insert (\"Hello\", \"World\") to greetings;";
    memdb::core::ParsedQuery pq_insert = parser.parse(insert_query);
    db.insert_row(pq_insert.table_name, *pq_insert.insert_values);

    std::string select_concat = "select first + \" \" + second as full_greeting from greetings;";
    memdb::core::QueryResult result_concat = db.execute(select_concat);
    ASSERT_TRUE(result_concat.is_ok());
    ASSERT_EQ(result_concat.get_data().size(), 1);
    EXPECT_EQ(result_concat.get_data()[0][0]->get_string(), "Hello World");
}

TEST(SelectTest, StringConcatenationTypeMismatch) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_query = "create table mix_types (name : string[10], age : int32);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);

    std::string insert_query = "insert (\"Alice\", 30) to mix_types;";
    memdb::core::ParsedQuery pq_insert = parser.parse(insert_query);
    db.insert_row(pq_insert.table_name, *pq_insert.insert_values);

    std::string select_concat_invalid = "select name + age as invalid_concat from mix_types;";
    memdb::core::QueryResult result = db.execute(select_concat_invalid);

    ASSERT_FALSE(result.is_ok());
    EXPECT_THAT(result.get_error(), ::testing::HasSubstr("Operator '+' not supported for given types."));
}

TEST(SelectTest, OperatorPrecedence) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_query = "create table precedence (a : int32, b : int32, c : bool);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);

    std::vector<std::string> insert_queries = {
        "insert (2, 3, true) to precedence;",
        "insert (5, 10, false) to precedence;"
    };
    for (const auto& query : insert_queries) {
        memdb::core::ParsedQuery pq_insert = parser.parse(query);
        db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
    }

    std::string select_query = "select a + b * 2 + 1 as calculation from precedence where (a + b * 2 + 1) < 10 && c;";
    memdb::core::QueryResult result = db.execute(select_query);
    ASSERT_TRUE(result.is_ok());

    ASSERT_EQ(result.get_data().size(), 1);
    EXPECT_EQ(result.get_data()[0][0]->get_int(), 2 + 3 * 2 + 1);

    select_query = "select (a + b) * 2 + 1 as calculation from precedence where (a + b) * 2 > 10 && c;";
    result = db.execute(select_query);
    ASSERT_TRUE(result.is_ok());

    ASSERT_EQ(result.get_data().size(), 0);

    std::string insert_query_new = "insert (4, 4, true) to precedence;";
    memdb::core::ParsedQuery pq_insert_new = parser.parse(insert_query_new);
    db.insert_row(pq_insert_new.table_name, *pq_insert_new.insert_values);

    result = db.execute(select_query);
    ASSERT_TRUE(result.is_ok());
    ASSERT_EQ(result.get_data().size(), 1);
    EXPECT_EQ(result.get_data()[0][0]->get_int(), (4 + 4) * 2 + 1);
}

TEST(SelectTest, NestedParentheses) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_query = "create table nested (x : int32, y : int32, z : bool);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);

    std::vector<std::string> insert_queries = {
        "insert (1, 2, true) to nested;",
        "insert (3, 4, false) to nested;",
        "insert (5, 6, true) to nested;"
    };
    for (const auto& query : insert_queries) {
        memdb::core::ParsedQuery pq_insert = parser.parse(query);
        db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
    }

    std::string select_query = "select x, y from nested where ((x + y) * 2) > 5 && !(z || false);";
    memdb::core::QueryResult result = db.execute(select_query);
    ASSERT_TRUE(result.is_ok());

    ASSERT_EQ(result.get_data().size(), 1);
    EXPECT_EQ(result.get_data()[0][0]->get_int(), 3);
    EXPECT_EQ(result.get_data()[0][1]->get_int(), 4);
}

TEST(SelectTest, UnbalancedParentheses) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_query = "create table errors (a : int32, b : int32);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);

    std::string insert_query = "insert (1, 2) to errors;";
    memdb::core::ParsedQuery pq_insert = parser.parse(insert_query);
    db.insert_row(pq_insert.table_name, *pq_insert.insert_values);

    std::string select_query = "select a from errors where (a + b > 2;";
    memdb::core::QueryResult result = db.execute(select_query);

    ASSERT_FALSE(result.is_ok());
    EXPECT_THAT(result.get_error(), ::testing::HasSubstr("Unbalanced parentheses or braces"));
}

TEST(SelectTest, InvalidExpressions) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_query = "create table invalid (a : int32, b : int32);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);

    std::string insert_query = "insert (1, 2) to invalid;";
    memdb::core::ParsedQuery pq_insert = parser.parse(insert_query);
    db.insert_row(pq_insert.table_name, *pq_insert.insert_values);

    std::string select_missing_op = "select a b from invalid;";
    memdb::core::QueryResult result_missing_op = db.execute(select_missing_op);
    ASSERT_FALSE(result_missing_op.is_ok());
    EXPECT_THAT(result_missing_op.get_error(), ::testing::HasSubstr("Unexpected token in expression."));

    std::string select_extra_op = "select a + + b from invalid;";
    memdb::core::QueryResult result_extra_op = db.execute(select_extra_op);
    ASSERT_FALSE(result_extra_op.is_ok());
    EXPECT_THAT(result_extra_op.get_error(), ::testing::HasSubstr("Unexpected token in expression"));

    std::string select_unbalanced = "select (a + b from invalid;";
    memdb::core::QueryResult result_unbalanced = db.execute(select_unbalanced);
    ASSERT_FALSE(result_unbalanced.is_ok());
    EXPECT_THAT(result_unbalanced.get_error(), ::testing::HasSubstr("Unbalanced parentheses or braces"));

    std::string select_unknown_op = "select a ** b as invalid_op from invalid;";
    memdb::core::QueryResult result_unknown_op = db.execute(select_unknown_op);
    ASSERT_FALSE(result_unknown_op.is_ok());
    EXPECT_THAT(result_unknown_op.get_error(), ::testing::HasSubstr("Unexpected token in expression."));

    std::string select_length_invalid = "select |a| as length_a from invalid;";
    memdb::core::QueryResult result_length_invalid = db.execute(select_length_invalid);
    ASSERT_FALSE(result_length_invalid.is_ok());
    EXPECT_THAT(result_length_invalid.get_error(), ::testing::HasSubstr("Operator '|var|' requires String or Bytes type"));

    std::string select_logical_invalid = "select a && b as invalid_logical from invalid;";
    memdb::core::QueryResult result_logical_invalid = db.execute(select_logical_invalid);
    ASSERT_FALSE(result_logical_invalid.is_ok());
    EXPECT_THAT(result_logical_invalid.get_error(), ::testing::HasSubstr("Operator '&&' requires Bool types"));
}

TEST(SelectTest, CombinedExpressions) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);

    std::string create_query = "create table combined (a : int32, b : int32, c : int32, name : string[20], flag : bool);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);

    std::vector<std::string> insert_queries = {
        "insert (1, 2, 3, \"Test\", true) to combined;",
        "insert (4, 5, 6, \"Example\", false) to combined;",
        "insert (7, 8, 9, \"SampleData\", true) to combined;"
    };
    for (const auto& query : insert_queries) {
        memdb::core::ParsedQuery pq_insert = parser.parse(query);
        db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
    }

    std::string select_query = "select a, b, c, name from combined where (a + b) > c && |name| < 10 && flag;";
    memdb::core::QueryResult result = db.execute(select_query);
    ASSERT_TRUE(result.is_ok());

    ASSERT_EQ(result.get_data().size(), 0);

    std::string insert_query_new = "insert (5, 6, 10, \"Short\", true) to combined;";
    memdb::core::ParsedQuery pq_insert_new = parser.parse(insert_query_new);
    db.insert_row(pq_insert_new.table_name, *pq_insert_new.insert_values);

    result = db.execute(select_query);
    ASSERT_TRUE(result.is_ok());
    ASSERT_EQ(result.get_data().size(), 1);
    EXPECT_EQ(result.get_data()[0][0]->get_int(), 5);
    EXPECT_EQ(result.get_data()[0][1]->get_int(), 6);
    EXPECT_EQ(result.get_data()[0][2]->get_int(), 10);
    EXPECT_EQ(result.get_data()[0][3]->get_string(), "Short");
}