#include <iostream>
#include <sstream>
#include <string>
#include <cctype>
#include <unordered_map>
#include <vector>

// Function to split a line into words
std::vector<std::string> splitWords(const std::string &line) {
    std::vector<std::string> words;
    std::string word;
    for (char ch : line) {
        if (std::isalnum(ch)) {  // Include alphanumeric characters
            word += ch;
        } else if (!word.empty()) {
            words.push_back(word);
            word.clear();
        }
    }
    if (!word.empty()) {
        words.push_back(word);
    }
    return words;
}

int main() {
    std::string inputLine;
    while (std::getline(std::cin, inputLine)) {
        // Find the delimiter between key and value
        size_t delimiterPos = inputLine.find(", ");
        if (delimiterPos == std::string::npos) {
            std::cerr << "Invalid input format: " << inputLine << std::endl;
            continue;
        }

        // Extract the <file content> portion
        std::string fileContent = inputLine.substr(delimiterPos + 2);

        // Split the file content into words
        std::vector<std::string> words = splitWords(fileContent);

        // Output each word with a count of 1
        for (const std::string &word : words) {
            std::cout << word << "\t1" << std::endl;
        }
    }

    return 0;
}
