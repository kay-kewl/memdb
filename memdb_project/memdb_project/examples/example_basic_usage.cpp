#include <iostream>
#include "memdb/core/Database.h"
#include "memdb/core/QueryParser.h"

int main() {
    memdb::core::Database db;

    std::string create_users_table = R"(
        create table users (
            {key, autoincrement} id : int32,
            {unique} login : string[32],
            password_hash : bytes[8],
            is_admin : bool = false
        );
    )";
    auto result = db.execute(create_users_table);
    if (!result.is_ok()) {
        std::cerr << "Error creating 'users' table: " << result.get_error() << "\n";
        return 1;
    }

    std::string create_posts_table = R"(
        create table posts (
            {key, autoincrement} id : int32,
            user_id : int32,
            text : string[256]
        );
    )";
    result = db.execute(create_posts_table);
    if (!result.is_ok()) {
        std::cerr << "Error creating 'posts' table: " << result.get_error() << "\n";
        return 1;
    }

    result = db.execute(R"(insert (, "vasya", 0xdeadbeefdeadbeef) to users;)");
    if (!result.is_ok()) {
        std::cerr << "Error inserting into 'users': " << result.get_error() << "\n";
        return 1;
    }

    result = db.execute(R"(insert (, "admin", 0x0000000000000000, true) to users;)");
    if (!result.is_ok()) {
        std::cerr << "Error inserting into 'users': " << result.get_error() << "\n";
        return 1;
    }

    result = db.execute(R"(insert (, 1, "Hello, world!") to posts;)");
    result = db.execute(R"(insert (, 1, "My second post") to posts;)");
    result = db.execute(R"(insert (, 2, "Admin's post") to posts;)");

    result = db.execute(R"(create ordered index on users by login;)");
    result = db.execute(R"(create unordered index on posts by user_id;)");

    auto select_query = "select posts.id, users.login, posts.text from users join posts on users.id = posts.user_id where true;";
    result = db.execute(select_query);
    if (result.is_ok()) {
        for (const auto& row : result.get_data()) {
            int post_id = row[0]->get_int();
            std::string_view login = row[1]->get_string();
            std::string_view text = row[2]->get_string();
            std::cout << "Post ID: " << post_id << ", User: " << login << ", Text: " << text << "\n";
        }
    } else { 
        std::cerr << "Error executing select query: " << result.get_error() << "\n";
    }

    result = db.execute(R"(update users set is_admin = true where login = "vasya";)");
    if (!result.is_ok()) {
        std::cerr << "Error updating 'users': " << result.get_error() << "\n";
    }

    result = db.execute(R"(delete posts where id = 2;)");
    if (!result.is_ok()) {
        std::cerr << "Error deleting from 'posts': " << result.get_error() << "\n";
    }

    std::cout << "Saved db.bin, trying to reopen and read from it" << std::endl;

    db.save_to_file("db.bin");

    memdb::core::Database db2;
    db2.load_from_file("db.bin");
    auto result2 = db2.execute(select_query);
    if (result2.is_ok()) {
        for (const auto& row : result2.get_data()) {
            int post_id = row[0]->get_int();
            std::string_view login = row[1]->get_string();
            std::string_view text = row[2]->get_string();
            std::cout << "Post ID: " << post_id << ", User: " << login << ", Text: " << text << "\n";
        }
    }

    return 0;
}