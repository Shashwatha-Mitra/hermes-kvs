#include <vector>
#include <string>
#include <iostream>
#include <fstream>
#include <sstream>

std::vector<std::string> parseConfigFile(const std::string& config_file) {
    std::vector<std::string> servers;
    std::ifstream file(config_file);
    int num_partitions;

    std::cout << "parsing config file\n";

    if (!file.is_open()) {
        std::cerr << "Error: Could not open file " << config_file << std::endl;
        std::exit(1);
    }
    std::string line;
    while (std::getline(file, line)) {
        std::stringstream ss(line);
        std::string port;
        std::getline(ss, port, '\n');
        servers.push_back("localhost:"+port);
    }

    file.close();
    
    std::cout << "parsing config file done\n";
    return servers;
}