#include "query5.hpp"
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <map>
#include <thread>
#include <mutex>
#include <algorithm>
#include <unordered_map>

std::vector<std::string> split(const std::string &line, char delimiter = '|')
{
    std::vector<std::string> tokens;
    std::string token;
    std::stringstream ss(line);

    while (std::getline(ss, token, delimiter))
    {
        tokens.push_back(token);
    }
    return tokens;
}

// Function to parse command line arguments
bool parseArgs(int argc, char *argv[],
               std::string &r_name,
               std::string &start_date,
               std::string &end_date,
               int &num_threads,
               std::string &table_path,
               std::string &result_path)
{
    if (argc < 13)
    {
        return false;
    }

    for (int i = 1; i < argc; i++)
    {
        std::string arg = argv[i];

        if (arg == "--r_name" && i + 1 < argc)
        {
            r_name = argv[++i];
        }
        else if (arg == "--start_date" && i + 1 < argc)
        {
            start_date = argv[++i];
        }
        else if (arg == "--end_date" && i + 1 < argc)
        {
            end_date = argv[++i];
        }
        else if (arg == "--threads" && i + 1 < argc)
        {
            num_threads = std::stoi(argv[++i]);
        }
        else if (arg == "--table_path" && i + 1 < argc)
        {
            table_path = argv[++i];
        }
        else if (arg == "--result_path" && i + 1 < argc)
        {
            result_path = argv[++i];
        }
        else
        {
            return false;
        }
    }

    return true;
}

// Function to read TPCH data from the specified paths
bool readTPCHData(
    const std::string &table_path,
    std::vector<std::map<std::string, std::string>> &customer_data,
    std::vector<std::map<std::string, std::string>> &orders_data,
    std::vector<std::map<std::string, std::string>> &lineitem_data,
    std::vector<std::map<std::string, std::string>> &supplier_data,
    std::vector<std::map<std::string, std::string>> &nation_data,
    std::vector<std::map<std::string, std::string>> &region_data)
{
    std::string line;

    // ---------- region.tbl ----------
    {
        std::ifstream file(table_path + "/region.tbl");
        if (!file.is_open())
            return false;

        while (std::getline(file, line))
        {
            auto c = split(line);
            if (c.size() < 2)
                continue;

            region_data.push_back({{"r_regionkey", c[0]},
                                   {"r_name", c[1]}});
        }
        file.close();
    }

    // ---------- nation.tbl ----------
    {
        std::ifstream file(table_path + "/nation.tbl");
        if (!file.is_open())
            return false;

        while (std::getline(file, line))
        {
            auto c = split(line);
            if (c.size() < 3)
                continue;

            nation_data.push_back({{"n_nationkey", c[0]},
                                   {"n_name", c[1]},
                                   {"n_regionkey", c[2]}});
        }
        file.close();
    }

    // ---------- supplier.tbl ----------
    {
        std::ifstream file(table_path + "/supplier.tbl");
        if (!file.is_open())
            return false;

        while (std::getline(file, line))
        {
            auto c = split(line);
            if (c.size() < 4)
                continue;

            supplier_data.push_back({{"s_suppkey", c[0]},
                                     {"s_nationkey", c[3]}});
        }
        file.close();
    }

    // ---------- customer.tbl ----------
    {
        std::ifstream file(table_path + "/customer.tbl");
        if (!file.is_open())
            return false;

        while (std::getline(file, line))
        {
            auto c = split(line);
            if (c.size() < 4)
                continue;

            customer_data.push_back({{"c_custkey", c[0]},
                                     {"c_nationkey", c[3]}});
        }
        file.close();
    }

    // ---------- orders.tbl ----------
    {
        std::ifstream file(table_path + "/orders.tbl");
        if (!file.is_open())
            return false;

        while (std::getline(file, line))
        {
            auto c = split(line);
            if (c.size() < 5)
                continue;

            orders_data.push_back({{"o_orderkey", c[0]},
                                   {"o_custkey", c[1]},
                                   {"o_orderdate", c[4]}});
        }
        file.close();
    }

    // ---------- lineitem.tbl ----------
    {
        std::ifstream file(table_path + "/lineitem.tbl");
        if (!file.is_open())
            return false;

        while (std::getline(file, line))
        {
            auto c = split(line);
            if (c.size() < 7)
                continue;

            lineitem_data.push_back({{"l_orderkey", c[0]},
                                     {"l_suppkey", c[2]},
                                     {"l_extendedprice", c[5]},
                                     {"l_discount", c[6]}});
        }
        file.close();
    }

    return true;
}

// Function to execute TPCH Query 5 using multithreading
bool executeQuery5(
    const std::string &r_name,
    const std::string &start_date,
    const std::string &end_date,
    int num_threads,
    const std::vector<std::map<std::string, std::string>> &customer_data,
    const std::vector<std::map<std::string, std::string>> &orders_data,
    const std::vector<std::map<std::string, std::string>> &lineitem_data,
    const std::vector<std::map<std::string, std::string>> &supplier_data,
    const std::vector<std::map<std::string, std::string>> &nation_data,
    const std::vector<std::map<std::string, std::string>> &region_data,
    std::map<std::string, double> &results)
{
    // ---------- Build JOIN MAPS ----------
    std::unordered_map<std::string, std::string> regionKeyToName;
    for (const auto &r : region_data)
        regionKeyToName[r.at("r_regionkey")] = r.at("r_name");

    std::unordered_map<std::string, std::pair<std::string, std::string>> nationInfo;
    for (const auto &n : nation_data)
        nationInfo[n.at("n_nationkey")] = {n.at("n_name"), n.at("n_regionkey")};

    std::unordered_map<std::string, std::string> supplierToNation;
    for (const auto &s : supplier_data)
        supplierToNation[s.at("s_suppkey")] = s.at("s_nationkey");

    std::unordered_map<std::string, std::string> customerToNation;
    for (const auto &c : customer_data)
        customerToNation[c.at("c_custkey")] = c.at("c_nationkey");

    std::unordered_map<std::string, std::pair<std::string, std::string>> orderInfo;
    for (const auto &o : orders_data)
        orderInfo[o.at("o_orderkey")] = {o.at("o_custkey"), o.at("o_orderdate")};

    // ---------- Multithreading Setup ----------
    std::mutex mtx;
    int total_rows = lineitem_data.size();
    int chunk_size = total_rows / num_threads;

    auto worker = [&](int start, int end)
    {
        std::map<std::string, double> local_result;

        for (int i = start; i < end; i++)
        {
            const auto &l = lineitem_data[i];

            std::string orderKey = l.at("l_orderkey");
            std::string suppKey = l.at("l_suppkey");

            // Join: lineitem → orders
            if (!orderInfo.count(orderKey))
                continue;
            auto orderPair = orderInfo[orderKey];
            std::string custKey = orderPair.first;
            std::string orderDate = orderPair.second;

            // Date filter
            if (!(orderDate >= start_date && orderDate < end_date))
                continue;

            // Join: orders → customer
            if (!customerToNation.count(custKey))
                continue;
            std::string custNation = customerToNation[custKey];

            // Join: lineitem → supplier
            if (!supplierToNation.count(suppKey))
                continue;
            std::string suppNation = supplierToNation[suppKey];

            // Nation consistency
            if (custNation != suppNation)
                continue;

            // Join: nation → region
            if (!nationInfo.count(suppNation))
                continue;
            auto nationPair = nationInfo[suppNation];
            std::string nationName = nationPair.first;
            std::string regionKey = nationPair.second;

            // Region filter
            if (!regionKeyToName.count(regionKey))
                continue;
            if (regionKeyToName[regionKey] != r_name)
                continue;

            // Revenue calculation
            double price = std::stod(l.at("l_extendedprice"));
            double discount = std::stod(l.at("l_discount"));
            double revenue = price * (1.0 - discount);

            local_result[nationName] += revenue;
        }

        // Merge local results
        std::lock_guard<std::mutex> lock(mtx);
        for (const auto &p : local_result)
            results[p.first] += p.second;
    };

    // ---------- Launch Threads ----------
    std::vector<std::thread> threads;
    int start = 0;

    for (int t = 0; t < num_threads; t++)
    {
        int end = (t == num_threads - 1) ? total_rows : start + chunk_size;
        threads.emplace_back(worker, start, end);
        start = end;
    }

    for (auto &t : threads)
        t.join();

    return true;
}

// Function to output results to the specified path
bool outputResults(const std::string &result_path,
                   const std::map<std::string, double> &results)
{
    std::ofstream out(result_path + "/query5_result.txt");
    if (!out.is_open())
    {
        return false;
    }

    for (const auto &entry : results)
    {
        out << entry.first << " | " << entry.second << std::endl;
        std::cout << entry.first << " | " << entry.second << std::endl;
    }

    out.close();
    return true;
}
