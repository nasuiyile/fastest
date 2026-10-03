
#include "server/config.h"
#include "server/fastest_server.h"
#include "server/comm/mailbox.h"
#include "server/comm/worker.h"
int main() {
	// const Config cfg;
	// fast_server::FastestServer server(cfg);
	// server.start();
	comm::Worker::create_workers();
}