#include <iostream>
#include <string>
#include <sstream>
#include <regex>
#include <vector>

int main() {
    const std::regex PATTERN_X("Parking"); // Replace with your actual regex pattern
    std::string line;

    while (std::getline(std::cin, line)) {
        // Tokenize the CSV line
        std::stringstream ss(line);
        std::string field;
        std::string value;

        std::getline(ss, field, ',');

        value = line.substr(field.length() + 1);

        // // Ensure the line has enough fields
        // if (fields.size() < 18) {
        //     continue; // Skip lines with insufficient fields
        // }

        // 1:source, -9822752.01226842,4887653.93470103,1,Streetname - Mast Arm,"16"" X 42""", ,Traffic Signal Mast Arm, ,Streetname, ,D3-1,Champaign,1,,AERIAL,L,Mercury Dr,1,,{14F48419-04A7-4932-A850-884CA5DC67FA}

        // Join the entire line into a single string for regex matching
        if (std::regex_search(value, PATTERN_X)) {
            // Print OBJECTID and Sign_Type
            std::cout << line << std::endl;
        }
    }

    return 0;
}
