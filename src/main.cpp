

#include "server/comm/handler.h"
#include "server/comm/worker.h"
int main() {
	// const Config cfg;
	// fast_server::FastestServer server(cfg);
	// server.start();
	const fastest::Config config {.port = 2333, .addr = "0.0.0.0"};
	const auto handlers = fastest::Handler::create_handlers(config);
	for (const auto &handler : handlers) {
		handler->start();
	}
	for (const auto &handler : handlers) {
		handler->join();
	}
	spdlog::info("FastestServer stopped.");
}