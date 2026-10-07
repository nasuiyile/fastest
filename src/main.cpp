
#include "server/config.h"
#include "server/fastest_server.h"
#include "server/comm/handler.h"
#include "server/comm/mailbox.h"
#include "server/comm/worker.h"
int main() {
	// const Config cfg;
	// fast_server::FastestServer server(cfg);
	// server.start();
	const comm::Config config {.port = 2333, .addr = "0.0.0.0"};
	const auto handlers = comm::Handler::create_handlers(config);
	for (const auto &handler : handlers) {
		handler->start();
	}
	for (const auto &handler : handlers) {
		handler->join();
	}
	spdlog::info("FastestServer stopped.");
}