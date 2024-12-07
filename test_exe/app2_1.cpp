#include <iostream>
#include <string>
#include <sstream>
#include <unordered_map>
#include <vector>

int main() {
    // Define the parameter X and set it to one of the allowed types
    const std::string SIGN_POST_TYPE_X = "Punched Telespar"; // Change to desired type as needed

    std::string line;
    while (std::getline(std::cin, line)) {
        // Tokenize the CSV line
        std::stringstream ss(line);
        std::vector<std::string> fields;
        std::string field;


        // 1:source, -9822752.01226842,4887653.93470103,1,Streetname - Mast Arm,"16"" X 42""", ,Punched Telespar, ,Streetname, ,D3-1,Champaign,1,,AERIAL,L,Mercury Dr,1,,{14F48419-04A7-4932-A850-884CA5DC67FA}

        while (std::getline(ss, field, ',')) {
            fields.push_back(field);
        }

        // Ensure the line has enough fields
        if (fields.size() < 18) {
            continue; // Skip lines with insufficient fields
        }

        // Check if the 'Sign_Post' matches the parameter X
        const std::string& sign_post = fields[7];
        if (sign_post == SIGN_POST_TYPE_X) {
            // Increment the count for the corresponding 'Category'
            const std::string& category = fields[9];

            // Print the updated count for this category
            std::cout << category << " " << 1 << std::endl;
        }
    }

    return 0;
}
