#include "ts_engine.h"
#include <iostream>
#include <csignal>
#include <string>

static rtf::TSEngine* g_engine = nullptr;
static void signal_handler(int) {
    if (g_engine) g_engine->stop();
}

int main(int argc, char* argv[]) {
    rtf::EngineConfig cfg;

    // 简单命令行参数解析
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--journal-dir" && i + 1 < argc)
            cfg.journal_dir = argv[++i];
        else if (arg == "--output-dir" && i + 1 < argc)
            cfg.csv_output_dir = argv[++i];
        else if (arg == "--dump-interval" && i + 1 < argc)
            cfg.dump_interval_seconds = std::stoi(argv[++i]);
        else if (arg == "--reader-name" && i + 1 < argc)
            cfg.reader_name = argv[++i];
        else if (arg == "--help") {
            std::cout << "Usage: realtime_factors [options]\n"
                      << "  --journal-dir DIR    Journal directory (default: /shared/kungfu/journal/user/)\n"
                      << "  --output-dir DIR     CSV output directory (default: /tmp/rtf_output/)\n"
                      << "  --dump-interval N    Dump interval in seconds (default: 60)\n"
                      << "  --reader-name NAME   Paged reader name (default: rtf_engine)\n"
                      << std::endl;
            return 0;
        }
    }

    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    rtf::TSEngine engine(cfg);
    g_engine = &engine;

    std::cerr << "[main] starting TSEngine..."
              << " journal=" << cfg.journal_dir
              << " output=" << cfg.csv_output_dir
              << " interval=" << cfg.dump_interval_seconds << "s"
              << std::endl;

    engine.run();
    return 0;
}
