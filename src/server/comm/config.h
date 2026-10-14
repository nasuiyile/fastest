
#pragma once
#include <string>
namespace fastest {
struct Config {
	unsigned short port = 2333;
	std::string addr = "0.0.0.0";
};
} // namespace comm