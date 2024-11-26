#include "memdb/core/Database.h"
#include "memdb/core/Value.h"

#include "memdb/core/exceptions/DatabaseException.h"

#include "memdb/core/QueryResult.h"

#include <iostream>
#include <iomanip>
#include <vector>
#include <string>
#include <unordered_map>

void print_query_result(const memdb::core::QueryResult& result) {
    const std::vector<std::vector<std::optional<memdb::core::Value>>>& data = result.get_data();
    for (const auto& row : data) {
        for (const auto& val : row) {
            if (val.has_value()) {
                std::cout << val->to_string() << "\t";
            } else {
                std::cout << "NULL\t";
            }
        }
        std::cout << "\n";
    }
}


int main()
{
    memdb::core::Database db;
    memdb::core::QueryResult result;

    result = db.execute("cREAte table tEsT_nuMbErs (x : int32, y : int32, z : int32)");
    if (!result.is_ok())
    {
        std::cerr << "Error creating table 'users': " << result.get_error() << std::endl;
        return 1;
    }
    print_query_result(result);
    std::cout << "Success1" << std::endl;

    result = db.execute("Insert (10, 20, 30) to tEsT_nuMbErs");
    if (!result.is_ok())
    {
        std::cerr << "Error creating table 'users': " << result.get_error() << std::endl;
        return 1;
    }
    std::cout << "Success2" << std::endl;

    result = db.execute("iNSeRt (15, 25, 35) to tEsT_nuMbErs");
    if (!result.is_ok())
    {
        std::cerr << "Error creating table 'users': " << result.get_error() << std::endl;
        return 1;
    }
    std::cout << "Success3" << std::endl;

    result = db.execute("insert (-5, -10, -15) to tEsT_nuMbErs");
    if (!result.is_ok())
    {
        std::cerr << "Error creating table 'users': " << result.get_error() << std::endl;
        return 1;
    }
    std::cout << "Success4" << std::endl;

    result = db.execute("create table test_strings (s1 : string[50], s2 : string[50])");
    if (!result.is_ok())
    {
        std::cerr << "Error creating table 'users': " << result.get_error() << std::endl;
        return 1;
    }
    std::cout << "Success5" << std::endl;

    result = db.execute("insert (\"hello\", \"world\") to test_strings");
    if (!result.is_ok())
    {
        std::cerr << "Error creating table 'users': " << result.get_error() << std::endl;
        return 1;
    }
    std::cout << "Success6" << std::endl;

    result = db.execute("insert (\"foo\", \"bar\") to test_strings");
    if (!result.is_ok())
    {
        std::cerr << "Error creating table 'users': " << result.get_error() << std::endl;
        return 1;
    }
    std::cout << "Success7" << std::endl;

    result = db.execute("create table test_bools (b1 : bool, b2 : bool)");
    if (!result.is_ok())
    {
        std::cerr << "Error creating table 'users': " << result.get_error() << std::endl;
        return 1;
    }
    std::cout << "Success8" << std::endl;

    result = db.execute("insert (true, false) to test_bools");
    if (!result.is_ok())
    {
        std::cerr << "Error creating table 'users': " << result.get_error() << std::endl;
        return 1;
    }
    std::cout << "Success9" << std::endl;

    result = db.execute("insert (false, true) to test_bools");
    if (!result.is_ok())
    {
        std::cerr << "Error creating table 'users': " << result.get_error() << std::endl;
        return 1;
    }
    std::cout << "Success10" << std::endl;

    result = db.execute("create table test_bytes (data1 : bytes[8], data2 : bytes[8])");
    if (!result.is_ok())
    {
        std::cerr << "Error creating table 'users': " << result.get_error() << std::endl;
        return 1;
    }
    std::cout << "Success11" << std::endl;

    result = db.execute("insert (0x0102030405060708, 0x0807060504030201) to test_bytes");
    if (!result.is_ok())
    {
        std::cerr << "Error creating table 'users': " << result.get_error() << std::endl;
        return 1;
    }
    std::cout << "Success12" << std::endl;





    result = db.execute("select x + y as sum, x - y as diff, x * y as prod, x / y as div, x % y as mod from tEsT_nuMbErs");
    if (!result.is_ok())
    {
        std::cerr << "Error creating table 'users': " << result.get_error() << std::endl;
        return 1;
    }
    print_query_result(result);
    std::cout << "Success13" << std::endl;

    result = db.execute("select x, y, x < y as x_less_y, x = y as x_eq_y, x > y as x_greater_y from tEsT_nuMbErs");
    if (!result.is_ok())
    {
        std::cerr << "Error creating table 'users': " << result.get_error() << std::endl;
        return 1;
    }
    print_query_result(result);
    std::cout << "Success14" << std::endl;

    result = db.execute("select s1, s2, s1 < s2 as s1_less_s2, s1 = s2 as s1_eq_s2, s1 > s2 as s1_greater_s2 from test_strings");
    if (!result.is_ok())
    {
        std::cerr << "Error creating table 'users': " << result.get_error() << std::endl;
        return 1;
    }
    print_query_result(result);
    std::cout << "Success15" << std::endl;

    result = db.execute("select b1, b2, b1 < b2 as b1_less_b2, b1 = b2 as b1_eq_b2, b1 > b2 as b1_greater_b2 from test_bools");
    if (!result.is_ok())
    {
        std::cerr << "Error creating table 'users': " << result.get_error() << std::endl;
        return 1;
    }
    print_query_result(result);
    std::cout << result << std::endl;
    std::cout << "Success16" << std::endl;

    result = db.execute("select b1, b2, b1 && b2 as b1_and_b2, b1 || b2 as b1_or_b2, !b1 as not_b1, b1 ^^ b2 as b1_xor_b2 from test_bools");
    if (!result.is_ok())
    {
        std::cerr << "Error creating table 'users': " << result.get_error() << std::endl;
        return 1;
    }
    print_query_result(result);
    std::cout << "Success17" << std::endl;

    result = db.execute("select s1, |s1| as len_s1, s2, |s2| as len_s2 from test_strings");
    if (!result.is_ok())
    {
        std::cerr << "Error creating table 'users': " << result.get_error() << std::endl;
        return 1;
    }
    print_query_result(result);
    std::cout << "Success18" << std::endl;

    result = db.execute("select data1, |data1| as len_data1, data2, |data2| as len_data2 from test_bytes");
    if (!result.is_ok())
    {
        std::cerr << "Error: " << result.get_error() << std::endl;
        return 1;
    }
    print_query_result(result);
    std::cout << "Success19" << std::endl;

    result = db.execute("select s1, s2, s1 + s2 as concatenated from test_strings");
    if (!result.is_ok())
    {
        std::cerr << "Error creating table 'users': " << result.get_error() << std::endl;
        return 1;
    }
    print_query_result(result);
    std::cout << "Success20" << std::endl;

    result = db.execute("select x, y, z, (x + y) * z as with_parens from tEsT_nuMbErs");
    if (!result.is_ok())
    {
        std::cerr << "Error: " << result.get_error() << std::endl;
        return 1;
    }
    print_query_result(result);
    std::cout << "Success21" << std::endl;

    result = db.execute("select b1, b2, b1 < b2 as b1_less_b2, b1 = b2 as b1_eq_b2, b1 > b2 as b1_greater_b2 from test_bools");
    if (!result.is_ok())
    {
        std::cerr << "Error: " << result.get_error() << std::endl;
        return 1;
    }
    print_query_result(result);
    std::cout << "Success22" << std::endl;

    result = db.execute("select b1, b2, b1 && b2 as b1_and_b2, b1 || b2 as b1_or_b2, !b1 as not_b1, b1 ^^ b2 as b1_xor_b2 from test_bools");
    if (!result.is_ok())
    {
        std::cerr << "Error creating table 'users': " << result.get_error() << std::endl;
        return 1;
    }
    print_query_result(result);
    std::cout << "Success23" << std::endl;

    result = db.execute("select s1, |s1| as len_s1, s2, |s2| as len_s2 from test_strings");
    if (!result.is_ok())
    {
        std::cerr << "Error creating table 'users': " << result.get_error() << std::endl;
        return 1;
    }
    std::cout << result << std::endl;
    std::cout << "Success24" << std::endl;
}