//
// Created by 12968 on 2026/9/19.
//

#ifndef FASTEST_MEMCACHEDREQUEST_H
#define FASTEST_MEMCACHEDREQUEST_H

#include <boost/asio/streambuf.hpp>
#include <string>
#include <string_view>
#include <istream>

namespace protocol {
    struct MemcachedRequest {
        std::string command; // "set", "get", "delete" ...
        std::string key;
        std::string value; // 二进制安全，可能含 \r\n
        // 其他字段按需扩展：flags, exptime, bytes, noreply
        std::string flags;
        std::string exptime;
        std::size_t bytes = 0;
        bool noreply = false;
        bool has_value = false; // 是否为存储类命令
    };

    static bool is_storage_command(std::string_view cmd) {
        return cmd == "set" || cmd == "add" || cmd == "replace" ||
               cmd == "append" || cmd == "prepend" || cmd == "cas";
    }

    static bool is_read_command(std::string_view cmd) {
        return cmd == "get" || cmd == "gets" || cmd == "gat" || cmd == "gats";
    }

    static bool is_delete_command(std::string_view cmd) {
        return cmd == "delete";
    }

    // 从 streambuf 中按 \r\n 提取一行（不含 \r\n）
    static std::string extract_line(boost::asio::streambuf &buf) {
        std::istream is(&buf);
        std::string line;
        std::getline(is, line); // 读到 '\n'
        if (!line.empty() && line.back() == '\r')
            line.pop_back(); // 去掉 '\r'
        return line;
    }

    // 按空格切分命令行
    static std::vector<std::string_view> split_ws(std::string_view line) {
        std::vector<std::string_view> tokens;
        std::size_t i = 0;
        while (i < line.size()) {
            while (i < line.size() && line[i] == ' ') ++i;
            std::size_t start = i;
            while (i < line.size() && line[i] != ' ') ++i;
            if (i > start) tokens.emplace_back(line.substr(start, i - start));
        }
        return tokens;
    }
}


#endif //FASTEST_MEMCACHEDREQUEST_H
