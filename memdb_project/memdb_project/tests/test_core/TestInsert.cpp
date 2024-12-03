#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "memdb/core/QueryParser.h"
#include "memdb/core/Database.h"
#include "memdb/core/Table.h"

TEST(InsertTest, SuccessfulInsert) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);
    std::string create_query = "create table users (id : int32, name: string[32], age: int32, is_active: bool);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);
    
    std::string insert_query = "insert (1, \"Alice\", 30, true) to users;";
    memdb::core::ParsedQuery pq_insert = parser.parse(insert_query);
    
    EXPECT_NO_THROW(db.insert_row(pq_insert.table_name, *pq_insert.insert_values));
}

TEST(InsertTest, AutoIncrementedID) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);
    
    std::string create_query = "create table users ({key, autoincrement} id : int32, name: string[32]);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);
    
    std::string insert_query1 = "insert (, \"Alice\") to users;";
    std::string insert_query2 = "insert (, \"Bob\") to users;";
    
    EXPECT_NO_THROW({
        memdb::core::ParsedQuery pq_insert1 = parser.parse(insert_query1);
        db.insert_row(pq_insert1.table_name, *pq_insert1.insert_values);
        
        memdb::core::ParsedQuery pq_insert2 = parser.parse(insert_query2);
        db.insert_row(pq_insert2.table_name, *pq_insert2.insert_values);
    });
    
    auto table = db.get_table("users");
    ASSERT_EQ(table->get_all_rows().size(), 2);
    
    const auto& row1 = table->get_row(1);
    const auto& row2 = table->get_row(2);
    
    EXPECT_EQ(row1.get_value(0)->get_int(), 1);
    EXPECT_EQ(row1.get_value(1)->get_string(), "Alice");
    
    EXPECT_EQ(row2.get_value(0)->get_int(), 2);
    EXPECT_EQ(row2.get_value(1)->get_string(), "Bob");
}

TEST(InsertTest, InsertWithUniqueConstraint) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);
    
    std::string create_query = "create table users ({key, autoincrement} id : int32, {unique} email: string[50]);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);
    
    std::string insert_query1 = "insert (, \"alice@example.com\") to users;";
    std::string insert_query2 = "insert (, \"bob@example.com\") to users;";
    std::string insert_query3 = "insert (, \"alice@example.com\") to users;";
    
    EXPECT_NO_THROW({
        memdb::core::ParsedQuery pq_insert1 = parser.parse(insert_query1);
        db.insert_row(pq_insert1.table_name, *pq_insert1.insert_values);
        
        memdb::core::ParsedQuery pq_insert2 = parser.parse(insert_query2);
        db.insert_row(pq_insert2.table_name, *pq_insert2.insert_values);
    });
    
    try {
        memdb::core::ParsedQuery pq_insert3 = parser.parse(insert_query3);
        db.insert_row(pq_insert3.table_name, *pq_insert3.insert_values);
        FAIL() << "Expected exception for duplicate unique column value.";
    } catch (const std::invalid_argument& e) {
        EXPECT_THAT(e.what(), testing::HasSubstr("Duplicate value for unique/key column \"email\""));
    }
}

TEST(InsertTest, StringExceedsSize) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);
    
    std::string create_query = "create table users (id : int32, name: string[5]);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);
    
    std::string insert_query = "insert (1, \"ExceedsSize\") to users;";
    try {
        memdb::core::ParsedQuery pq_insert = parser.parse(insert_query);
        db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
        FAIL() << "Expected exception for string exceeding defined size.";
    } catch (const std::invalid_argument& e) {
        EXPECT_THAT(e.what(), testing::HasSubstr("String value exceeds defined size of 5"));
    }
}

TEST(InsertTest, BytesExceedsSize) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);
    
    std::string create_query = "create table data_table (id : int32, data: bytes[4]);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);
    
    std::string insert_query = "insert (1, 0x1234567890) to data_table;";
    try {
        memdb::core::ParsedQuery pq_insert = parser.parse(insert_query);
        db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
        FAIL() << "Expected exception for bytes exceeding defined size.";
    } catch (const std::invalid_argument& e) {
        EXPECT_THAT(e.what(), testing::HasSubstr("Bytes value exceeds defined size of 4"));
    }
}

TEST(InsertTest, MissingValues) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);
    
    std::string create_query = "create table users (id : int32, name: string[32], age: int32);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);
    
    std::string insert_query = "insert (1, \"Bob\") to users;";
    try {
        memdb::core::ParsedQuery pq_insert = parser.parse(insert_query);
        db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
        FAIL() << "Expected exception for missing values.";
    } catch (const std::invalid_argument& e) {
        EXPECT_THAT(e.what(), testing::HasSubstr("Missing value for column: age"));
    }
}

TEST(InsertTest, ExtraValues) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);
    
    std::string create_query = "create table users (id : int32, name: string[32]);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);
    
    std::string insert_query = "insert (1, \"Charlie\", 25) to users;";
    try {
        memdb::core::ParsedQuery pq_insert = parser.parse(insert_query);
        db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
        FAIL() << "Expected exception for extra values.";
    } catch (const std::invalid_argument& e) {
        EXPECT_THAT(e.what(), testing::HasSubstr("Too many values for table columns"));
    }
}

TEST(InsertTest, InvalidDataTypes) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);
    
    std::string create_query = "create table users (id : int32, name: string[32], is_active: bool);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);
    
    std::string insert_query = "insert (2, \"David\", \"true\") to users;";
    try {
        memdb::core::ParsedQuery pq_insert = parser.parse(insert_query);
        db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
        FAIL() << "Expected exception for invalid data type.";
    } catch (const std::invalid_argument& e) {
        EXPECT_THAT(e.what(), testing::HasSubstr("Invalid type for column 'is_active'"));
    }
}

TEST(InsertTest, ReservedKeywordAsValue) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);
    
    std::string create_query = "create table users (id : int32, keyword_field: string[10]);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);
    
    std::string insert_query = "insert (3, \"select\") to users;";
    EXPECT_NO_THROW({
        memdb::core::ParsedQuery pq_insert = parser.parse(insert_query);
        db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
    });
}

TEST(InsertTest, InsertIntoNonExistentTable) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);
    
    std::string insert_query = "insert (1, \"Eve\", 28) to nonexistent_table;";
    try {
        memdb::core::ParsedQuery pq_insert = parser.parse(insert_query);
        db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
        FAIL() << "Expected exception for inserting into non-existent table.";
    } catch (const std::invalid_argument& e) {
        EXPECT_THAT(e.what(), testing::HasSubstr("Table does not exist: nonexistent_table"));
    }
}

TEST(InsertTest, InsertWithDefaultValues) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);
    
    std::string create_query = "create table users (id : int32, name: string[32], age: int32 = 25);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);
    
    std::string insert_query = "insert (1, \"Frank\") to users;";
    EXPECT_NO_THROW({
        memdb::core::ParsedQuery pq_insert = parser.parse(insert_query);
        db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
    });
    
    auto table = db.get_table("users");
    ASSERT_EQ(table->get_all_rows().size(), 1);
}

TEST(InsertTest, InsertWithSpecialCharacters) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);
    
    std::string create_query = "create table users (id : int32, bio: string[100]);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);
    
    std::string insert_query = "insert (1, \"Bio with (parentheses) and {braces}\") to users;";
    EXPECT_NO_THROW({
        memdb::core::ParsedQuery pq_insert = parser.parse(insert_query);
        db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
    });
}

TEST(InsertTest, InsertNullValues) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);
    
    std::string create_query = "create table users (id : int32, name: string[32], email: string[50] = \"\");";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);
    
    std::string insert_query = "insert (2, \"Grace\") to users;";
    EXPECT_NO_THROW({
        memdb::core::ParsedQuery pq_insert = parser.parse(insert_query);
        db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
    });

    auto table = db.get_table("users");
    ASSERT_EQ(table->get_all_rows().size(), 1);
    const auto& row = table->get_row(1); 

    auto email_value = row.get_value(2);
    ASSERT_TRUE(email_value.has_value());
    EXPECT_EQ(email_value->get_string(), ""); 
}

TEST(InsertTest, NamedColumnInsert) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);
    
    std::string create_query = "create table users ({key, autoincrement} id : int32, login: string[32], password_hash: bytes[8]);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);
    
    std::string insert_query = "insert (login = \"vasya\", password_hash = 0xdeadbeef) to users;";
    EXPECT_NO_THROW({
        memdb::core::ParsedQuery pq_insert = parser.parse(insert_query);
        db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
    });
}

TEST(InsertTest, MultilineInsert) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);
    
    std::string create_query = "create table users ({key, autoincrement} id : int32, login: string[32], is_admin: bool);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);
    
    std::string insert_query = R"(insert (
        login = "admin",
        is_admin = true
    ) to users;)";
    
    EXPECT_NO_THROW({
        memdb::core::ParsedQuery pq_insert = parser.parse(insert_query);
        db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
    });

    insert_query = R"(insert (
        ,
        "admin",
        true
    ) to users;)";
    
    EXPECT_NO_THROW({
        memdb::core::ParsedQuery pq_insert = parser.parse(insert_query);
        db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
    });
}

TEST(InsertTest, EmptyValuesForDefaults) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);
    
    std::string create_query = "create table users ({key, autoincrement} id : int32, login: string[32], is_admin: bool = false);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);
    
    std::string insert_query = "insert (, \"bob\", ) to users;";
    EXPECT_NO_THROW({
        memdb::core::ParsedQuery pq_insert = parser.parse(insert_query);
        db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
    });
    
    auto table = db.get_table("users");
    const auto& row = table->get_row(1);
    EXPECT_EQ(row.get_value(1)->get_string(), "bob");
    EXPECT_EQ(row.get_value(2)->get_bool(), false);
}

TEST(InsertTest, DuplicateColumnNamesInNamedInsert) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);
    
    std::string create_query = "create table users ({key, autoincrement} id : int32, username: string[32], email: string[64]);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);
    
    std::string insert_query = "insert (username = \"john_doe\", email = \"john@example.com\", username = \"johnny\") to users;";
    try {
        memdb::core::ParsedQuery pq_insert = parser.parse(insert_query);
        db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
        FAIL() << "Expected exception for duplicate column names in insert.";
    } catch (const std::invalid_argument& e) {
        EXPECT_THAT(e.what(), testing::HasSubstr("Duplicate column name: username"));
    }
}

TEST(InsertTest, InsertWithInvalidColumnNames) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);
    
    std::string create_query = "create table products ({key, autoincrement} id : int32, name: string[50], price: int32);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);
    
    std::string insert_query = "insert (name = \"Laptop\", cost = 1500) to products;";
    try {
        memdb::core::ParsedQuery pq_insert = parser.parse(insert_query);
        db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
        FAIL() << "Expected exception for invalid column name in insert.";
    } catch (const std::invalid_argument& e) {
        EXPECT_THAT(e.what(), testing::HasSubstr("Column not found: cost"));
    }
}

TEST(InsertTest, InsertWithEscapedCharacters) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);
    
    std::string create_query = "create table messages ({key, autoincrement} id : int32, content: string[100]);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);
    
    std::string insert_query = "insert (, \"Hello, \\\"World\\\"!\\nNew Line\") to messages;";
    EXPECT_NO_THROW({
        memdb::core::ParsedQuery pq_insert = parser.parse(insert_query);
        db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
    });
    
    auto table = db.get_table("messages");
    ASSERT_EQ(table->get_all_rows().size(), 1);
    const auto& row = table->get_row(1);
    
    EXPECT_EQ(row.get_value(1)->get_string(), "Hello, \"World\"!\nNew Line");
}

TEST(InsertTest, InsertMinMaxInt32) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);
    
    std::string create_query = "create table numbers (id : int32, value: int32);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);
    
    std::string insert_query_min = "insert (1, -2147483648) to numbers;";
    std::string insert_query_max = "insert (2, 2147483647) to numbers;";
    
    EXPECT_NO_THROW({
        memdb::core::ParsedQuery pq_insert_min = parser.parse(insert_query_min);
        db.insert_row(pq_insert_min.table_name, *pq_insert_min.insert_values);
        
        memdb::core::ParsedQuery pq_insert_max = parser.parse(insert_query_max);
        db.insert_row(pq_insert_max.table_name, *pq_insert_max.insert_values);
    });
    
    auto table = db.get_table("numbers");
    ASSERT_EQ(table->get_all_rows().size(), 2);
    
    const auto& row_min = table->get_row(1);
    const auto& row_max = table->get_row(2);
    
    EXPECT_EQ(row_min.get_value(1)->get_int(), -2147483648);
    EXPECT_EQ(row_max.get_value(1)->get_int(), 2147483647);
}

TEST(InsertTest, InsertEmptyStringsAndBytes) {
    memdb::core::Database db;
    memdb::core::QueryParser parser;
    parser.set_database(&db);
    
    std::string create_query = "create table test (id : int32, name: string[10], data: bytes[4]);";
    memdb::core::ParsedQuery pq_create = parser.parse(create_query);
    db.create_table(pq_create.table_name, pq_create.columns);
    
    std::string insert_query = "insert (1, \"\", 0x0000) to test;";
    EXPECT_NO_THROW({
        memdb::core::ParsedQuery pq_insert = parser.parse(insert_query);
        db.insert_row(pq_insert.table_name, *pq_insert.insert_values);
    });
    
    auto table = db.get_table("test");
    ASSERT_EQ(table->get_all_rows().size(), 1);
    
    const auto& row = table->get_row(1);
    EXPECT_EQ(row.get_value(1)->get_string(), "");
    EXPECT_EQ(row.get_value(2)->get_bytes(), std::vector<uint8_t>({0x00, 0x00}));
}
