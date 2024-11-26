// #include "memdb/core/Database.h"
// #include "memdb/core/exceptions/DatabaseException.h"

// #include <iostream>

// int main() {
//     memdb::core::Database db;

//     try {
//         // Создание таблицы users
//         std::string create_users_query = R"(
//             create table users (
//                 {autoincrement, key} id : int32,
//                 {unique} login : string[32],
//                 password_hash : bytes[8],
//                 is_admin : bool = false
//             )
//         )";

//         memdb::core::QueryResult result = db.execute(create_users_query);
//         if (!result.is_ok()) {
//             std::cerr << "Error creating table 'users': " << result.get_error() << std::endl;
//             return 1;
//         }
//         std::cout << "Table 'users' created successfully." << std::endl;

//         // Создание таблицы posts
//         std::string create_posts_query = R"(
//             create table posts (
//                 {autoincrement, key} id : int32,
//                 user_id : int32,
//                 content : string[255]
//             )
//         )";

//         result = db.execute(create_posts_query);
//         if (!result.is_ok()) {
//             std::cerr << "Error creating table 'posts': " << result.get_error() << std::endl;
//             return 1;
//         }
//         std::cout << "Table 'posts' created successfully." << std::endl;

//         // Вставка данных в таблицу users
//         std::string insert_users_query = R"(
//             insert (, "john_doe", 0xdeadbeefdeadbeef, false) to users
//         )";
//         result = db.execute(insert_users_query);
//         if (!result.is_ok()) {
//             std::cerr << "Error inserting into 'users': " << result.get_error() << std::endl;
//             return 1;
//         }
//         std::cout << "Inserted 'john_doe' into 'users'." << std::endl;

//         // Inserting 'alice' and 'bob' into 'users'
//         result = db.execute(R"(insert (, "alice", 0xabcdefabcdefabcd, false) to users)");
//         if (!result.is_ok()) {
//             std::cerr << "Error inserting 'alice' into 'users': " << result.get_error() << std::endl;
//             return 1;
//         }
//         result = db.execute(R"(insert (, "bob", 0x1234567812345678, true) to users)");
//         if (!result.is_ok()) {
//             std::cerr << "Error inserting 'bob' into 'users': " << result.get_error() << std::endl;
//             return 1;
//         }
//         std::cout << "Inserted 'alice' and 'bob' into 'users'." << std::endl;

//         // Inserting into 'posts'
//         result = db.execute(R"(insert (, 1, "Hello, world!") to posts)");
//         if (!result.is_ok()) {
//             std::cerr << "Error inserting into 'posts': " << result.get_error() << std::endl;
//             // return 1;
//         }
//         result = db.execute(R"(insert (, 2, "This is a post from Alice.") to posts)");
//         if (!result.is_ok()) {
//             std::cerr << "Error inserting into 'posts': " << result.get_error() << std::endl;
//             // return 1;
//         }
//         result = db.execute(R"(insert (, 1, "Another post from John.") to posts)");
//         if (!result.is_ok()) {
//             std::cerr << "Error inserting into 'posts': " << result.get_error() << std::endl;
//             // return 1;
//         }
//         std::cout << "Inserted posts into 'posts'." << std::endl;

//         // Selecting all data from 'posts' to verify insertions
//         std::string select_posts_query = "select id, user_id, content from posts";
//         result = db.execute(select_posts_query);
//         if (!result.is_ok()) {
//             std::cerr << "Error selecting from 'posts': " << result.get_error() << std::endl;
//             return 1;
//         }
//         std::cout << "Data in 'posts' table:" << std::endl;
//         const auto& posts_data = result.get_data();
//         for (const auto& row : posts_data) {
//             int32_t id = row[0]->get_int();
//             int32_t user_id = row[1]->get_int();
//             std::string content = row[2]->get_string();
//             std::cout << "ID: " << id << ", User ID: " << user_id << ", Content: " << content << std::endl;
//         }

//         // Создание индекса
//         std::string create_index_query = R"(
//             create ordered index on users by login
//         )";
//         result = db.execute(create_index_query);
//         if (!result.is_ok()) {
//             std::cerr << "Error creating index on 'users': " << result.get_error() << std::endl;
//             return 1;
//         }
//         std::cout << "Index on 'users' created successfully." << std::endl;

//         // Выполнение SELECT запроса
//         std::string select_query = R"(
//             select id, login, is_admin from users where is_admin = false
//         )";
//         result = db.execute(select_query);
//         if (!result.is_ok()) {
//             std::cerr << "Error selecting from 'users': " << result.get_error() << std::endl;
//             return 1;
//         }
//         std::cout << "Selected users where is_admin = false:" << std::endl;
//         const auto& data = result.get_data();
//         for (const auto& row : data) {
//             int32_t id = row[0]->get_int();
//             std::string login = row[1]->get_string();
//             bool is_admin = row[2]->get_bool();
//             std::cout << "ID: " << id << ", Login: " << login << ", Is Admin: " << (is_admin ? "true" : "false") << std::endl;
//         }

//         // Выполнение UPDATE запроса
//         std::string update_query = R"(
//             update users set is_admin = true where login = "john_doe"
//         )";
//         result = db.execute(update_query);
//         if (!result.is_ok()) {
//             std::cerr << "Error updating 'users': " << result.get_error() << std::endl;
//             return 1;
//         }
//         std::cout << "Updated 'john_doe' to be an admin." << std::endl;

//         // Выполнение DELETE запроса
//         std::string delete_query = R"(
//             delete from users where login = "alice"
//         )";
//         result = db.execute(delete_query);
//         if (!result.is_ok()) {
//             std::cerr << "Error deleting from 'users': " << result.get_error() << std::endl;
//             return 1;
//         }
//         std::cout << "Deleted 'alice' from 'users'." << std::endl;

//         // Выполнение JOIN запроса
//         std::string join_query = R"(
//             select posts.id, users.login, posts.content from users join posts on users.id = posts.user_id where true
//         )";
//         result = db.execute(join_query);
//         if (!result.is_ok()) {
//             std::cerr << "Error executing join query: " << result.get_error() << std::endl;
//             return 1;
//         }
//         std::cout << "Joined 'users' and 'posts':" << std::endl;
//         const auto& join_data = result.get_data();
//         for (const auto& row : join_data) {
//             int32_t post_id = row[0]->get_int();
//             std::string user_login = row[1]->get_string();
//             std::string content = row[2]->get_string();
//             std::cout << "Post ID: " << post_id << ", User: " << user_login << ", Content: " << content << std::endl;
//         }

//         // Сохранение базы данных в файл
//         std::string save_filename = "db.bin";
//         db.save_to_file(save_filename);
//         std::cout << "Database saved to file: " << save_filename << std::endl;

//         // Загрузка базы данных из файла
//         memdb::core::Database loaded_db;
//         loaded_db.load_from_file(save_filename);
//         std::cout << "Database loaded from file: " << save_filename << std::endl;

//         // Вывод структуры загруженной базы данных
//         std::cout << "Loaded Database Structure:" << std::endl;
//         std::cout << loaded_db.to_string() << std::endl;

//     } catch (const memdb::core::exceptions::DatabaseException& e) {
//         std::cerr << "Database Error: " << e.what() << std::endl;
//         return 1;
//     } catch (const std::exception& e) {
//         std::cerr << "Standard Exception: " << e.what() << std::endl;
//         return 1;
//     }

//     return 0;
// }