
#pragma once
#include <string>
namespace comm {
struct Config {
	unsigned short port = 2333;
	std::string addr = "0.0.0.0";
};
} // namespace comm