#include <gtest/gtest.h>
#include "memdb/core/QueryParser.h"
#include "memdb/core/Database.h"
#include "memdb/core/Table.h"

TEST(CreateTableTest, SuccessfulCreation) {
    memdb::core::Database db;
    std::string query = "create table users ({key, autoincrement} id : int32, {unique} login: string[32], password_hash: bytes[8], is_admin: bool = false);";
    
    try {
        memdb::core::ParsedQuery pq = memdb::core::QueryParser().parse(query);
        ASSERT_EQ(pq.type, memdb::core::ParsedQuery::QueryType::CreateTable);
        db.create_table(pq.table_name, pq.columns); 
        
        ASSERT_TRUE(db.has_table("users"));
        auto table = db.get_table("users");
        ASSERT_EQ(table->get_name(), "users");
        ASSERT_EQ(table->get_columns().size(), 4);
    }
    catch (const std::exception& e) {
        FAIL() << "Exception thrown during successful creation: " << e.what();
    }
}

TEST(CreateTableTest, DuplicateColumnNames) {
    memdb::core::Database db;
    std::string query = "create table users ({key, autoincrement} id : int32, {unique} id: string[32], password_hash: bytes[8]);";
    
    try {
        memdb::core::ParsedQuery pq = memdb::core::QueryParser().parse(query);
        db.create_table(pq.table_name, pq.columns); 
        FAIL() << "Expected exception for duplicate column names.";
    }
    catch (const std::invalid_argument& e) {
        EXPECT_STREQ(e.what(), "Duplicate column name: id");
    }
    catch (...) {
        FAIL() << "Expected std::invalid_argument for duplicate column names.";
    }
}

TEST(CreateTableTest, UnknownColumnType) {
    memdb::core::Database db;
    std::string query = "create table users ({key} id : int64, {unique} login: string[32]);";
    
    try {
        memdb::core::ParsedQuery pq = memdb::core::QueryParser().parse(query);
        db.create_table(pq.table_name, pq.columns); 
        FAIL() << "Expected exception for unknown column type.";
    }
    catch (const std::invalid_argument& e) {
        EXPECT_STREQ(e.what(), "Unknown column type: int64");
    }
    catch (...) {
        FAIL() << "Expected std::invalid_argument for unknown column type.";
    }
}

TEST(CreateTableTest, EmptyTableName) {
    memdb::core::Database db;
    std::string query = "create table () {key} id : int32;";
    
    try {
        memdb::core::ParsedQuery pq = memdb::core::QueryParser().parse(query);
        db.create_table(pq.table_name, pq.columns); 
        FAIL() << "Expected exception for empty table name.";
    }
    catch (const std::invalid_argument& e) {
        EXPECT_STREQ(e.what(), "Table name cannot be empty.");
    }
    catch (...) {
        FAIL() << "Expected std::invalid_argument for empty table name.";
    }
}

TEST(CreateTableTest, CreateTableWithNoColumns) {
    memdb::core::Database db;
    std::string query = "create table empty_table ();";
    
    try {
        memdb::core::ParsedQuery pq = memdb::core::QueryParser().parse(query);
        db.create_table(pq.table_name, pq.columns); 
        FAIL() << "Expected exception for creating table with no columns.";
    }
    catch (const std::invalid_argument& e) {
        EXPECT_STREQ(e.what(), "Column definitions cannot be empty");
    }
    catch (...) {
        FAIL() << "Expected std::invalid_argument for creating table with no columns.";
    }
}

TEST(CreateTableTest, InvalidColumnAttributes) {
    memdb::core::Database db;
    std::string query = "create table users ({invalid_attr} id : int32, login: string[32]);";
    
    try {
        memdb::core::ParsedQuery pq = memdb::core::QueryParser().parse(query);
        db.create_table(pq.table_name, pq.columns); 
        FAIL() << "Expected exception for invalid column attributes.";
    }
    catch (const std::invalid_argument& e) {
        EXPECT_STREQ(e.what(), "Unknown column attribute: invalid_attr");
    }
    catch (...) {
        FAIL() << "Expected std::invalid_argument for invalid column attributes.";
    }
}

TEST(CreateTableTest, UnbalancedParentheses) {
    memdb::core::Database db;
    std::string query = "create table users ({key} id : int32, login: string[32];"; 
    
    try {
        memdb::core::ParsedQuery pq = memdb::core::QueryParser().parse(query);
        db.create_table(pq.table_name, pq.columns); 
        FAIL() << "Expected exception for unbalanced parentheses.";
    }
    catch (const std::invalid_argument& e) {
        EXPECT_STREQ(e.what(), "Unbalanced parentheses or braces in query.");
    }
    catch (...) {
        FAIL() << "Expected std::invalid_argument for unbalanced parentheses.";
    }
}

TEST(CreateTableTest, MissingColon) {
    memdb::core::Database db;
    std::string query = "create table users ({key} id int32, login: string[32]);"; 
    
    try {
        memdb::core::ParsedQuery pq = memdb::core::QueryParser().parse(query);
        db.create_table(pq.table_name, pq.columns); 
        FAIL() << "Expected exception for missing colon in column definition.";
    }
    catch (const std::invalid_argument& e) {
        EXPECT_STREQ(e.what(), "Expected ':' in column definition.");
    }
    catch (...) {
        FAIL() << "Expected std::invalid_argument for missing colon in column definition.";
    }
}

TEST(CreateTableTest, InvalidDefaultValue) {
    memdb::core::Database db;
    std::string query = "create table users ({key} id : int32 = \"invalid\", login: string[32]);";
    
    try {
        memdb::core::ParsedQuery pq = memdb::core::QueryParser().parse(query);
        db.create_table(pq.table_name, pq.columns); 
        FAIL() << "Expected exception for invalid default value type.";
    }
    catch (const std::invalid_argument& e) {
        EXPECT_NE(std::string(e.what()), "Type mismatch for column 'id'.");
    }
    catch (...) {
        FAIL() << "Expected std::invalid_argument for invalid default value type.";
    }
}

TEST(CreateTableTest, MultipleTablesWithAttributes) {
    memdb::core::Database db;
    std::string query1 = "create table users ({key, autoincrement} id : int32, {unique} login: string[32], password_hash: bytes[8], is_admin: bool = false);";
    std::string query2 = "create table products ({key} product_id : int32, name: string[50], price: int32, {unique} sku: string[20]);";
    
    try {
        memdb::core::ParsedQuery pq1 = memdb::core::QueryParser().parse(query1);
        db.create_table(pq1.table_name, pq1.columns); 
        ASSERT_TRUE(db.has_table("users"));
        
        memdb::core::ParsedQuery pq2 = memdb::core::QueryParser().parse(query2);
        db.create_table(pq2.table_name, pq2.columns); 
        ASSERT_TRUE(db.has_table("products"));
        
        auto products_table = db.get_table("products");
        ASSERT_EQ(products_table->get_columns().size(), 4);
        ASSERT_EQ(products_table->get_columns()[0].get_name(), "product_id");
        ASSERT_EQ(products_table->get_columns()[0].get_type().get_type(), memdb::core::Type::Int32);
        ASSERT_TRUE(products_table->get_columns()[0].has_attribute(memdb::core::ColumnAttribute::Key));
    }
    catch (const std::exception& e) {
        FAIL() << "Exception thrown during multiple table creation: " << e.what();
    }
}

TEST(CreateTableTest, VariousDataTypes) {
    memdb::core::Database db;
    std::string query = "create table users (id : int32, name: string[50], data: bytes[16], is_active: bool);";
    
    try {
        memdb::core::ParsedQuery pq = memdb::core::QueryParser().parse(query);
        ASSERT_EQ(pq.columns.size(), 4);
        db.create_table(pq.table_name, pq.columns); 
        
        ASSERT_TRUE(db.has_table("users"));
        auto table = db.get_table("users");
        ASSERT_EQ(table->get_columns()[0].get_type().get_type(), memdb::core::Type::Int32);
        ASSERT_EQ(table->get_columns()[1].get_type().get_type(), memdb::core::Type::String);
        ASSERT_EQ(table->get_columns()[2].get_type().get_type(), memdb::core::Type::Bytes);
        ASSERT_EQ(table->get_columns()[3].get_type().get_type(), memdb::core::Type::Bool);
    }
    catch (const std::exception& e) {
        FAIL() << "Exception thrown during various data types creation: " << e.what();
    }
}

TEST(CreateTableTest, CaseInsensitiveKeywords) {
    memdb::core::Database db;
    std::string query = "CREATE TABLE users ({KEY, AUTOINCREMENT} ID : INT32, {UNIQUE} LOGIN: STRING[32]);";
    
    try {
        memdb::core::ParsedQuery pq = memdb::core::QueryParser().parse(query);
        db.create_table(pq.table_name, pq.columns); 
        
        ASSERT_TRUE(db.has_table("users"));
        auto table = db.get_table("users");
        ASSERT_EQ(table->get_columns().size(), 2);
        ASSERT_TRUE(table->get_columns()[0].has_attribute(memdb::core::ColumnAttribute::Key));
        ASSERT_TRUE(table->get_columns()[0].has_attribute(memdb::core::ColumnAttribute::AutoIncrement));
        ASSERT_TRUE(table->get_columns()[1].has_attribute(memdb::core::ColumnAttribute::Unique));
    }
    catch (const std::exception& e) {
        FAIL() << "Exception thrown during case-insensitive keywords creation: " << e.what();
    }
}

TEST(CreateTableTest, ColumnsWithoutAttributes) {
    memdb::core::Database db;
    std::string query = "create table users (id : int32, login: string[32], is_active: bool);";
    
    try {
        memdb::core::ParsedQuery pq = memdb::core::QueryParser().parse(query);
        db.create_table(pq.table_name, pq.columns); 
        
        ASSERT_TRUE(db.has_table("users"));
        auto table = db.get_table("users");
        ASSERT_EQ(table->get_columns().size(), 3);
        ASSERT_FALSE(table->get_columns()[0].has_attribute(memdb::core::ColumnAttribute::Key));
        ASSERT_FALSE(table->get_columns()[1].has_attribute(memdb::core::ColumnAttribute::Unique));
    }
    catch (const std::exception& e) {
        FAIL() << "Exception thrown during columns without attributes creation: " << e.what();
    }
}

TEST(CreateTableTest, ColumnsWithBracketsInString) {
    memdb::core::Database db;
    std::string query = "create table users (id : int32, description: string[100] = \"User with (parentheses) and {braces}\");";
    
    try {
        memdb::core::ParsedQuery pq = memdb::core::QueryParser().parse(query);
        db.create_table(pq.table_name, pq.columns); 
        
        ASSERT_TRUE(db.has_table("users"));
        auto table = db.get_table("users");
        ASSERT_EQ(table->get_columns().size(), 2);

        auto default_value = table->get_columns()[1].get_default_value();
        ASSERT_TRUE(default_value.has_value());
        ASSERT_EQ(default_value.value().to_string(), "\"User with (parentheses) and {braces}\"");
    }
    catch (const std::exception& e) {
        FAIL() << "Exception thrown during columns with brackets in string creation: " << e.what();
    }
}

TEST(CreateTableTest, MissingAttributes) {
    memdb::core::Database db;
    std::string query = "create table users (id int32, {unique} login: string[32]);";
    
    try {
        memdb::core::ParsedQuery pq = memdb::core::QueryParser().parse(query);
        db.create_table(pq.table_name, pq.columns); 
        FAIL() << "Expected exception for missing ':' in column definition.";
    }
    catch (const std::invalid_argument& e) {
        EXPECT_NE(std::string(e.what()).find("Expected ':' in column definition."), std::string::npos);
    }
    catch (...) {
        FAIL() << "Expected std::invalid_argument for missing ':' in column definition.";
    }
}

TEST(CreateTableTest, DefaultValuesVariousTypes) {
    memdb::core::Database db;
    std::string query = "create table test (id : int32 = -1, flag : bool = true, data : bytes[2] = 0xFF00, name: string[10] = \"default\");";

    try {
        memdb::core::ParsedQuery pq = memdb::core::QueryParser().parse(query);
        db.create_table(pq.table_name, pq.columns);
        auto table = db.get_table("test");

        ASSERT_EQ(table->get_columns()[0].get_default_value().value().get_int(), -1);
        ASSERT_EQ(table->get_columns()[1].get_default_value().value().get_bool(), true);
        ASSERT_EQ(table->get_columns()[2].get_default_value().value().get_bytes(), std::vector<uint8_t>({0xFF, 0x00}));
        ASSERT_EQ(table->get_columns()[3].get_default_value().value().get_string(), "default");

    } catch (const std::exception& e) {
        FAIL() << "Exception thrown: " << e.what();
    }
}

TEST(CreateTableTest, StringAndBytesSizes) {
    memdb::core::Database db;

    std::string query1 = "create table test (data : string[0]);";
    try {
        memdb::core::QueryParser().parse(query1);
        FAIL() << "Expected exception for zero-length string";
    } catch (const std::invalid_argument& e) {
        EXPECT_STREQ(e.what(), "Size can only be set for String and Bytes types with size > 0.");
    }

    std::string query2 = "create table test (data : bytes[1024]);";
    try {
        memdb::core::ParsedQuery pq = memdb::core::QueryParser().parse(query2);
        db.create_table(pq.table_name, pq.columns);
        ASSERT_TRUE(db.has_table("test"));
        auto table = db.get_table("test");
        ASSERT_EQ(table->get_columns()[0].get_type().get_size(), 1024);
    } catch (const std::exception& e) {
        FAIL() << "Exception thrown for large byte size: " << e.what();
    }
}

TEST(CreateTableTest, WhitespaceVariations) {
    memdb::core::Database db;
    std::string query = "   create                           table test     ({ key ,autoincrement}  id  :int32 ,{unique}name :  string[32]);";
    try {
        memdb::core::ParsedQuery pq = memdb::core::QueryParser().parse(query);
        db.create_table(pq.table_name, pq.columns);
        ASSERT_TRUE(db.has_table("test"));
        auto table = db.get_table("test");
        ASSERT_EQ(table->get_columns().size(), 2);
    } catch (const std::exception& e) {
        FAIL() << "Exception thrown for whitespace variations: " << e.what();
    }
}

TEST(CreateTableTest, EmptyAttributeList) {
    memdb::core::Database db;
    std::string query = "create table test ({ } id: int32);";
    try {
        memdb::core::ParsedQuery pq = memdb::core::QueryParser().parse(query);
        db.create_table(pq.table_name, pq.columns);
        ASSERT_TRUE(db.has_table("test"));
        auto table = db.get_table("test");
        ASSERT_EQ(table->get_columns().size(), 1);
    } catch (const std::invalid_argument& e) {
        EXPECT_STREQ(e.what(), "Unknown column attribute: ");
    }

}

TEST(CreateTableTest, MissingDefaultValue) {
    memdb::core::Database db;
    std::string query = "create table test (id: int32 = );";
    try {
        memdb::core::QueryParser().parse(query);
        FAIL() << "Expected exception for missing default value.";
    } catch (const std::invalid_argument& e) {
        EXPECT_NE(std::string(e.what()).find("Invalid default value"), std::string::npos);
    }
}

TEST(CreateTableTest, InvalidCharactersInNames) {
    memdb::core::Database db;
    std::string query1 = "create table test (my id : int32);";
    std::string query2 = "create table test-table (id : int32);";

    try {
        memdb::core::QueryParser().parse(query1);
        FAIL() << "Expected exception for invalid column name.";
    } catch (const std::invalid_argument& e) {
        EXPECT_NE(std::string(e.what()).find("Invalid column name"), std::string::npos);
    }

    try {
        memdb::core::QueryParser().parse(query2);
        FAIL() << "Expected exception for invalid table name.";
    } catch (const std::invalid_argument& e) {
        EXPECT_NE(std::string(e.what()).find("Invalid table name"), std::string::npos);
    }

}

TEST(CreateTableTest, CaseSensitivityTableNames) {
    memdb::core::Database db;
    std::string query1 = "create table MyTable (id : int32);";
    std::string query2 = "create table mytable (id : int32);";

    try {
        memdb::core::ParsedQuery pq1 = memdb::core::QueryParser().parse(query1);
        db.create_table(pq1.table_name, pq1.columns);

        memdb::core::ParsedQuery pq2 = memdb::core::QueryParser().parse(query2);
        db.create_table(pq2.table_name, pq2.columns); 

        ASSERT_TRUE(db.has_table("MyTable"));

    } catch (const std::invalid_argument& e) {
        FAIL() << "Exception thrown for case-sensitivity test: " << e.what();
    }
}

TEST(CreateTableTest, ReservedKeywordsAsTableNames) {
    memdb::core::Database db;
    std::string query = "create table create (id : int32);"; 

    try {
        memdb::core::QueryParser().parse(query);
        FAIL() << "Expected exception for reserved keyword as table name.";
    } catch (const std::invalid_argument& e) {
        EXPECT_NE(e.what(), "Reserved keyword used as table name: create.");
    }
}

TEST(CreateTableTest, ReservedKeywordAsColumnName) {
    memdb::core::Database db;
    std::string query = "create table test (select : int32, from: bool);";
    
    try {
        memdb::core::ParsedQuery pq = memdb::core::QueryParser().parse(query);
        db.create_table(pq.table_name, pq.columns); 
        FAIL() << "Expected exception for reserved keywords as column names.";
    }
    catch (const std::invalid_argument& e) {
        EXPECT_STREQ(e.what(), "Invalid column name: select");
    }
    catch (...) {
        FAIL() << "Expected std::invalid_argument for reserved keywords as column names.";
    }
}

TEST(CreateTableTest, TooLongStringPassed) {
    memdb::core::Database db;
    std::string query = "create table users (id : int32, description: string[10] = \"User with (parentheses) and {braces}\");";

    try {
        memdb::core::QueryParser().parse(query);
        FAIL() << "Expected exception for string exceeding defined size.";
    } catch (const std::invalid_argument& e) {
        EXPECT_STREQ(e.what(), "String value exceeds defined size of 10");
    }
}

TEST(CreateTableTest, TooLongBytesPassed) {
    memdb::core::Database db;
    std::string query = "create table users (id : int32, description: bytes[4] = 0x12345678AB);";

    try {
        memdb::core::QueryParser().parse(query);
        FAIL() << "Expected exception for bytes exceeding defined size.";
    } catch (const std::invalid_argument& e) {
        EXPECT_STREQ(e.what(), "Bytes value exceeds defined size of 4");
    }
}

TEST(CreateTableTest, CreateTableWithMixedAttributes) {
    memdb::core::Database db;
    std::string query = 
        "create table mixed_attrs ("
        "{key, autoincrement} id : int32, "
        "{unique} username: string[32], "
        "email: string[50] = \"\", "
        "{unique, key} phone: string[15]"
        ");";
    
    try {
        memdb::core::ParsedQuery pq = memdb::core::QueryParser().parse(query);
        db.create_table(pq.table_name, pq.columns); 
        
        ASSERT_TRUE(db.has_table("mixed_attrs"));
        auto table = db.get_table("mixed_attrs");
        ASSERT_EQ(table->get_columns().size(), 4);
        
        ASSERT_TRUE(table->get_columns()[0].has_attribute(memdb::core::ColumnAttribute::Key));
        ASSERT_TRUE(table->get_columns()[0].has_attribute(memdb::core::ColumnAttribute::AutoIncrement));
        
        ASSERT_TRUE(table->get_columns()[1].has_attribute(memdb::core::ColumnAttribute::Unique));
        
        ASSERT_FALSE(table->get_columns()[2].has_attribute(memdb::core::ColumnAttribute::Unique));
        ASSERT_TRUE(table->get_columns()[2].get_default_value().has_value());
        
        ASSERT_TRUE(table->get_columns()[3].has_attribute(memdb::core::ColumnAttribute::Unique));
        ASSERT_TRUE(table->get_columns()[3].has_attribute(memdb::core::ColumnAttribute::Key));
    }
    catch (const std::exception& e) {
        FAIL() << "Exception thrown during mixed attributes table creation: " << e.what();
    }
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}