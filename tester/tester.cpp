// load_test.cpp
// High-performance multi-threaded HTTP load testing tool.
// Target: POST /users on a local Go backend (default 127.0.0.1:8080).
//
// Build:  g++ -O3 -std=c++17 -pthread load_test.cpp -o load_test -lpthread
// Run:    ./load_test [num_threads] [duration_sec] [warmup_sec]
//         ./load_test              # 50 threads, 60s duration, 5s warmup (defaults)
//         ./load_test 100          # 100 threads, defaults otherwise
//         ./load_test 50 60 5      # explicit
//
// Design notes:
//   - Each request opens a fresh TCP connection with "Connection: close" and
//     reads until EOF. This sidesteps having to distinguish chunked transfer
//     encoding from Content-Length-delimited bodies, at the cost of paying
//     connect()/TCP-handshake overhead on every single request (which is
//     actually a realistic thing to measure for a server fielding many
//     short-lived clients). If you want persistent/keep-alive connections
//     instead, that's a reasonable follow-up but needs a proper HTTP
//     response parser (Content-Length + chunked support).
//   - Metrics are recorded per-thread with no locking during the run (each
//     thread appends to its own std::vector), and only merged after all
//     threads join. This avoids lock contention from skewing latency
//     numbers.
//   - The first `warmup_sec` seconds of wall-clock time are excluded from
//     every metric, both the per-second windows and the global summary.

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

using Clock = std::chrono::steady_clock;

struct RequestRecord {
    int second;        // relative second bucket since test start (0-based)
    double latency_ms; // 0.0 if status == 0 (connection-level failure)
    int status;         // HTTP status code, 0 = connect/send/recv failure
};

struct Config {
    std::string host = "127.0.0.1";
    int port = 8080;
    std::string path = "/users";
    int num_threads = 50;
    int duration_sec = 60;
    int warmup_sec = 5;
};

// ---------------------------------------------------------------------
// Networking
// ---------------------------------------------------------------------

// Performs a single POST request over a brand-new TCP connection.
// Returns {status_code, latency_ms}. status_code == 0 means the
// connection/send/recv failed before we got a parseable HTTP response.
static std::pair<int, double> doRequest(const Config& cfg, const std::string& body) {
    auto t0 = Clock::now();

    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return {0, 0.0};

    int flag = 1;
    setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(cfg.port);
    if (inet_pton(AF_INET, cfg.host.c_str(), &addr.sin_addr) != 1) {
        close(sock);
        return {0, 0.0};
    }

    if (connect(sock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        close(sock);
        return {0, 0.0};
    }

    std::ostringstream req;
    req << "POST " << cfg.path << " HTTP/1.1\r\n"
        << "Host: " << cfg.host << ":" << cfg.port << "\r\n"
        << "Content-Type: application/json\r\n"
        << "Content-Length: " << body.size() << "\r\n"
        << "Connection: close\r\n"
        << "\r\n"
        << body;
    const std::string reqStr = req.str();

    size_t sent = 0;
    while (sent < reqStr.size()) {
        ssize_t n = send(sock, reqStr.data() + sent, reqStr.size() - sent, 0);
        if (n <= 0) {
            close(sock);
            return {0, 0.0};
        }
        sent += static_cast<size_t>(n);
    }

    std::string response;
    response.reserve(512);
    char buf[4096];
    ssize_t n;
    while ((n = recv(sock, buf, sizeof(buf), 0)) > 0) {
        response.append(buf, static_cast<size_t>(n));
    }
    close(sock);

    auto t1 = Clock::now();
    double latency_ms = std::chrono::duration<double, std::milli>(t1 - t0).count();

    if (response.empty()) return {0, latency_ms};

    // Parse status line: "HTTP/1.1 201 Created\r\n..."
    int status = 0;
    size_t sp1 = response.find(' ');
    if (sp1 != std::string::npos) {
        size_t sp2 = response.find(' ', sp1 + 1);
        if (sp2 != std::string::npos) {
            try {
                status = std::stoi(response.substr(sp1 + 1, sp2 - sp1 - 1));
            } catch (...) {
                status = 0;
            }
        }
    }
    return {status, latency_ms};
}

// ---------------------------------------------------------------------
// Worker thread
// ---------------------------------------------------------------------

static void worker(int thread_id, const Config& cfg, Clock::time_point start,
                    std::vector<RequestRecord>& out) {
    std::mt19937_64 rng(std::random_device{}() ^ (thread_id * 2654435761u));
    std::uniform_int_distribution<int> dist(0, 999999999);

    out.reserve(4096);
    long counter = 0;

    while (true) {
        auto now = Clock::now();
        double elapsed = std::chrono::duration<double>(now - start).count();
        if (elapsed >= cfg.duration_sec) break;
        int sec_bucket = static_cast<int>(elapsed);

        std::ostringstream body;
        body << "{\"email\":\"loadtest_t" << thread_id << "_" << counter << "_"
             << dist(rng) << "@test.com\",\"name\":\"Load Test User\"}";
        counter++;

        auto result = doRequest(cfg, body.str());
        out.push_back({sec_bucket, result.second, result.first});
    }
}

// ---------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------

// Linear-interpolation percentile over a pre-sorted vector.
static double percentile(const std::vector<double>& sorted_latencies, double p) {
    if (sorted_latencies.empty()) return 0.0;
    double idx = p * static_cast<double>(sorted_latencies.size() - 1);
    size_t lo = static_cast<size_t>(std::floor(idx));
    size_t hi = static_cast<size_t>(std::ceil(idx));
    if (lo == hi) return sorted_latencies[lo];
    double frac = idx - static_cast<double>(lo);
    return sorted_latencies[lo] + (sorted_latencies[hi] - sorted_latencies[lo]) * frac;
}

int main(int argc, char** argv) {
    Config cfg;
    if (argc > 1) cfg.num_threads = std::max(1, std::atoi(argv[1]));
    if (argc > 2) cfg.duration_sec = std::max(1, std::atoi(argv[2]));
    if (argc > 3) cfg.warmup_sec = std::max(0, std::atoi(argv[3]));

    if (cfg.warmup_sec >= cfg.duration_sec) {
        std::cerr << "warmup_sec must be less than duration_sec\n";
        return 1;
    }

    std::cout << "=== HTTP Load Test ===\n"
              << "Target:    http://" << cfg.host << ":" << cfg.port << cfg.path << "\n"
              << "Threads:   " << cfg.num_threads << "\n"
              << "Duration:  " << cfg.duration_sec << "s (first " << cfg.warmup_sec
              << "s discarded as warmup)\n"
              << "=======================\n" << std::endl;

    std::vector<std::vector<RequestRecord>> perThreadResults(cfg.num_threads);
    std::vector<std::thread> threads;
    threads.reserve(cfg.num_threads);

    auto start = Clock::now();
    for (int i = 0; i < cfg.num_threads; ++i) {
        threads.emplace_back(worker, i, std::cref(cfg), start, std::ref(perThreadResults[i]));
    }
    for (auto& t : threads) t.join();
    auto end = Clock::now();
    double actual_duration = std::chrono::duration<double>(end - start).count();

    // Merge all per-thread records into one buffer, then bucket by second.
    std::vector<RequestRecord> all;
    {
        size_t total_est = 0;
        for (auto& v : perThreadResults) total_est += v.size();
        all.reserve(total_est);
    }
    for (auto& v : perThreadResults) all.insert(all.end(), v.begin(), v.end());

    std::map<int, std::vector<RequestRecord>> buckets;
    for (auto& r : all) buckets[r.second].push_back(r);

    // ---- Per-window (post-warmup) CSV: printed to stdout AND written to file ----
    std::ofstream csv("loadtest_results.csv");
    const char* header =
        "second,requests,rps,avg_latency_ms,p95_latency_ms,p99_latency_ms,success_count,error_count";
    std::cout << header << "\n";
    csv << header << "\n";

    std::vector<double> allValidLatencies; // status != 0, post-warmup, for global percentiles
    long total_success = 0, total_failed = 0, total_conn_errors = 0;

    for (int sec = cfg.warmup_sec; sec < cfg.duration_sec; ++sec) {
        auto it = buckets.find(sec);
        long count = 0, success = 0, error = 0;
        double sumLatency = 0.0;
        std::vector<double> latencies;

        if (it != buckets.end()) {
            for (auto& r : it->second) {
                count++;
                if (r.status == 0) {
                    error++;
                    total_conn_errors++;
                    continue; // connection-level failure: no latency signal
                }
                latencies.push_back(r.latency_ms);
                sumLatency += r.latency_ms;
                allValidLatencies.push_back(r.latency_ms);
                if (r.status >= 200 && r.status < 300) {
                    success++;
                    total_success++;
                } else {
                    error++;
                    total_failed++;
                }
            }
        }

        std::sort(latencies.begin(), latencies.end());
        double avg = latencies.empty() ? 0.0 : sumLatency / static_cast<double>(latencies.size());
        double p95 = percentile(latencies, 0.95);
        double p99 = percentile(latencies, 0.99);

        std::ostringstream line;
        line << sec << "," << count << "," << count << "," // 1s window => rps == count
             << std::fixed << std::setprecision(2) << avg << "," << p95 << "," << p99 << ","
             << success << "," << error;
        std::cout << line.str() << "\n";
        csv << line.str() << "\n";
    }
    csv.close();

    // ---- Global summary ----
    std::sort(allValidLatencies.begin(), allValidLatencies.end());
    long total_requests = total_success + total_failed + total_conn_errors;
    double measured_duration = cfg.duration_sec - cfg.warmup_sec;

    std::cout << "\n=== Global Summary (post-warmup, " << measured_duration << "s window) ===\n";
    std::cout << "Total requests:        " << total_requests << "\n";
    std::cout << "Successful (2xx):      " << total_success << "\n";
    std::cout << "Failed (non-2xx):      " << total_failed << "\n";
    std::cout << "Connection errors:     " << total_conn_errors << "\n";
    std::cout << std::fixed << std::setprecision(1);
    std::cout << "Overall throughput:    "
              << (measured_duration > 0 ? static_cast<double>(total_requests) / measured_duration : 0.0)
              << " req/s\n";
    std::cout << std::setprecision(2);
    std::cout << "Latency p50:           " << percentile(allValidLatencies, 0.50) << " ms\n";
    std::cout << "Latency p95:           " << percentile(allValidLatencies, 0.95) << " ms\n";
    std::cout << "Latency p99:           " << percentile(allValidLatencies, 0.99) << " ms\n";
    std::cout << "Latency p99.9:         " << percentile(allValidLatencies, 0.999) << " ms\n";
    std::cout << "\nActual wall-clock test duration: " << actual_duration << "s\n";
    std::cout << "Per-second CSV written to: loadtest_results.csv\n";

    return 0;
}