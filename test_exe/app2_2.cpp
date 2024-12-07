#include <iostream>
#include <sstream>
#include <string>
#include <unordered_map>

int main() {
    std::unordered_map<std::string, int> wordCounts; // Map to store accumulated counts
    std::string inputLine;

    while (std::getline(std::cin, inputLine)) {
        std::istringstream iss(inputLine);
        std::string word;
        int count;

        // Parse the input line in the format "<word>\t<count>"
        if (!(iss >> word >> count)) {
            // std::cerr << "Invalid input format: " << inputLine << std::endl;
            continue;
        }

        // Accumulate the count for the word
        wordCounts[word] += count;

        // Output the word and its accumulated count
        std::cout << word << "\t" << wordCounts[word] << std::endl;
    }

    return 0;
}
