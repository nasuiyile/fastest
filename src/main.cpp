
#include "server/config.h"
#include "server/fastest_server.h"

int main() {
	const Config cfg;
	fast_server::FastestServer server(cfg);
	server.start();
}