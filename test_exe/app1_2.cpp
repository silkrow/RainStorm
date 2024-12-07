#include <iostream>
#include <string>
#include <sstream>
#include <regex>
#include <vector>

int main() {
    const std::regex PATTERN_X("a"); // Replace with your actual regex pattern
    std::string line;

    while (std::getline(std::cin, line)) {
        // Tokenize the CSV line
        std::stringstream ss(line);
        std::vector<std::string> fields;
        std::string field;

        while (std::getline(ss, field, ',')) {
            fields.push_back(field);
        }

        // // Ensure the line has enough fields
        // if (fields.size() < 18) {
        //     continue; // Skip lines with insufficient fields
        // }

        // Join the entire line into a single string for regex matching
        
            // Print OBJECTID and Sign_Type
        std::cout << fields[3]  <<", "<< fields[4] << std::endl;
        
    }

    return 0;
}
