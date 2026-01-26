#include <stdlib.h>

#include <chrono>
#include <fstream>
#include <iostream>
#include <mutex>
#include <random>
#include <regex>
#include <string>
#include <vector>

#include "cpucounters.h"
#include "hash_api.h"
#include "timer.h"
#include "utils.h"

using namespace std;
using namespace pcm;
// #define LATENCY
// #define PM_PCM
enum { OP_INSERT, OP_READ, OP_DELETE, OP_UPDATE };
enum workload_type {
  YCSB,
  PiBench,
};
enum Hash {
  Halo_t,
  VIPER_t,
  CCEH_t,
  Dash_t,
  Clevel_t,
  PCLHT_t,
  SOFT_t,
};
#define VAL_LEN 3200
uint64_t LOAD_SIZE;
uint64_t RUN_SIZE;
uint64_t MAX_SIZE_LOAD = 200000000ULL;
uint64_t MAX_SIZE_RUN = 200000000ULL;
void run_test(workload_type wlt, int num_thread, string load_data,
              string run_data, hash_api *&h, string workload) {
  char values[] =
      "NvhE8N7yR26f4bbpMJnUKgHncH6QbsI10HyxlvYHKFiMk5nPNDbueF2xKLzteSd0NazU2APk"
      "JWXvBW2oUu8dkZnWMMu37G8TH2qm"
      "S0c8A9z41pxrC6ZU79OnfCZ06DsNXWY3U4dt1JTGQVvylBdZSlHWXC4PokCxsMdjv8xRptFM"
      "MQyHZRqMhNDnrsGKA12DEr7Zur0n"
      "tZpsyreMmPwuw7WMRnoN5wAYWtkqDwXyQlYb4RgtSc4xsonpTx2UhIUi15oJTx1CXAhmECac"
      "CQfntFnrSZt5qs1L64eeJ9Utus0N"
      "mKgEFV8qYDsNtJ21TkjCyCDhVIATkEugCw1BfNIB9AZDGiqXc0llp4rlJPl4bIG2QC4La3M1"
      "oh3yGlZTmdvN5pj1sIGkolpdoYVJ"
      "0NZM9KAo1d5sGFv9yGC7X0CTDOqyRu5c4NPktU70NbKqWNXa1kcaigIfeAuvJBs0Wso2osHz"
      "OjrbawgpfBPs1ePaWHgw7vbOcu9v"
      "Cqz1GnmdQw4mGSo4cc6tebQuKqLkQHuXa1MdRmzinBRoGQBQehqrDmmfNhcxfozcU7hOTjFA"
      "jryJ4HdSK57gOlrte5sZlvDW9rFd"
      "4OxG6WtFdZomRQPTNc4D9t7smqBR9EYDSjiAAqmIgZUiycHrlv6JQzEiexjqfGUbo8oJV6wi"
      "u7l3Jlfb94uByDxoexkMT5AjJzls"
      "er1dc9EfQz88q5Hv00g53Q3H6jcgicoY8YW5K4josd2e53ikesQi2kzqvTI9xxM5wtFexkFm"
      "8wFdMs6YmNpvNgTf37Hz204wX1Sf"
      "djFmCYEcP533LYcGB7CslEVMPYRZXHBT98XKtt8RqES7HBW65xSJRSj3qhIDUsgeu2Flo4Yq"
      "S68QoE69JzyBnwmmYw6uulVLVIAe"
      "iLl49oUhEiEjem8RrHPpEvrUoLDWwMdh14MfxwmEQbtGnUHEpRktUB6b7JTJN8OHBlLrvr71"
      "TkRK728ZgRv32rMZJ46O17qHTYc4"
      "AepNCGbpTII0J05OYiush6hiDo6H5pVHVUWy3nm7BBrBzEHVOCBMHNniw4CIzfavGLaUfgjl"
      "Bg0D4JBmYmkg0A4maCXsE9QTnGbA"
      "fQErGZkdMnRxXJ5EJ627e7zuFuVtazb0L65B3nU5R9tyUl2bTZiDcakK9evrTXoTkbkGjkCO"
      "iMSThGFScb6Lsgvl5wNCzlUZCxof"
      "jYQCLusRkXEm0CNVuifTnytctwLfKjwob4hJ0WxlQN9FV9Mm9zT61EQ8zEMrqr6hf7XMqhcQ"
      "R7DWAaf1fM4oNLIA7ZdKaspUaU6h"
      "oP2w3t3MktVaBp6MgS6Apbkb7EsihETHHqKFkKMCkYBbKfgsq7Jy49T1Wx2UJsD3XX03kVBb"
      "qRWmryYoMIqiCTCTqa0jIKzqQEnN";
  string insert("INSERT");
  string remove("REMOVE");
  string read("READ");
  string update("UPDATE");
  ifstream infile_load(load_data);
  string op;
  uint64_t key;
  uint64_t value_len;
  vector<uint64_t> init_keys(MAX_SIZE_LOAD);
  vector<uint64_t> keys(MAX_SIZE_RUN);
  vector<uint64_t> init_value_lens(MAX_SIZE_LOAD);
  vector<uint64_t> value_lens(MAX_SIZE_RUN);
  vector<int> ops(MAX_SIZE_RUN);
  int count = 0;
  while ((count < MAX_SIZE_LOAD) && infile_load.good()) {
    infile_load >> op >> key >> value_len;
    if (!op.size()) continue;
    if (op.size() && op.compare(insert) != 0) {
      cout << "READING LOAD FILE FAIL!\n";
      cout << op << endl;
      return;
    }
    init_keys[count] = key;
    init_value_lens[count] = value_len;
    count++;
  }
  LOAD_SIZE = count;
  infile_load.close();
  if (workload == "ycsbe") LOAD_SIZE = 0;
  fprintf(stderr, "Loaded %lu keys for initialing.\n", LOAD_SIZE);

  int *r = new int[1024];
  ifstream infile_run(run_data);
  count = 0;
  while ((count < MAX_SIZE_RUN) && infile_run.good()) {
    infile_run >> op >> key;
    if (op.compare(insert) == 0) {
      infile_run >> value_len;
      ops[count] = OP_INSERT;
      keys[count] = key;
      value_lens[count] = value_len;
    } else if (op.compare(update) == 0) {
      infile_run >> value_len;
      ops[count] = OP_UPDATE;
      keys[count] = key;
      value_lens[count] = value_len;
    } else if (op.compare(read) == 0) {
      ops[count] = OP_READ;
      keys[count] = key;
    } else if (op.compare(remove) == 0) {
      ops[count] = OP_DELETE;
      keys[count] = key;
    } else {
      continue;
    }
    count++;
  }
  RUN_SIZE = count;

#ifdef HALOT
#ifdef NONVAR
  Pair_t<size_t, size_t> *p = new Pair_t<size_t, size_t>[RUN_SIZE];
#elif VARVALUE
  Pair_t<size_t, std::string> *p = new Pair_t<size_t, std::string>[RUN_SIZE];
#else
  Pair_t<std::string, std::string> *p =
      new Pair_t<std::string, std::string>[RUN_SIZE];
#endif
#endif
  fprintf(stderr, "Loaded %d keys for running.\n", count);
  Timer tr;
  tr.start();
  h = new hash_api();
  printf("hash: %s %.1f ms.\n", h->hash_name().c_str(),
         tr.elapsed<std::chrono::milliseconds>());
#ifdef PM_PCM
  set_signal_handlers();
  PCM *m = PCM::getInstance();
  auto status = m->program();
  if (status != PCM::Success) {
    std::cout << "Error opening PCM: " << status << std::endl;
    if (status == PCM::PMUBusy)
      m->resetPMU();
    else
      exit(0);
  }
  print_cpu_details();
#endif
  auto part = LOAD_SIZE / num_thread;

  {
    // Load
    Timer sw;
    thread ths[num_thread];
    sw.start();
    auto insert = [&](size_t start, size_t len, int tid) {
      auto end = start + len;
#ifdef VIPERT

      auto c = h->get_client();
      for (size_t i = start; i < end; i++)
        h->insert(init_keys[i], init_value_lens[i],
                  reinterpret_cast<char *>(values), c);

#else
      for (size_t i = start; i < end; i++) {
        h->insert(init_keys[i], init_value_lens[i],
                  reinterpret_cast<char *>(values), tid, &r[i % 1024]);
      }

      h->wait();
#endif
    };
#ifdef PM_PCM
    auto before_state = getSystemCounterState();
#endif
    for (size_t i = 0; i < num_thread; i++) {
      ths[i] = thread(insert, part * i, part, i);
    }
    for (size_t i = 0; i < num_thread; i++) {
      ths[i].join();
    }
    auto t = sw.elapsed<std::chrono::milliseconds>();
    printf("Throughput: load, %f Mops/s\n",
           (LOAD_SIZE / 1000000.0) / (t / 1000.0));
#ifdef PM_PCM
    auto after_sstate = getSystemCounterState();
    cout << "MB ReadFromPMM: "
         << getBytesReadFromPMM(before_state, after_sstate) / 1000000 << " "
         << (getBytesReadFromPMM(before_state, after_sstate) / 1000000.0) /
                (t / 1000.0)
         << " MB/s" << endl;
    cout << "MB WrittenToPMM: "
         << getBytesWrittenToPMM(before_state, after_sstate) / 1000000 << " "
         << (getBytesWrittenToPMM(before_state, after_sstate) / 1000000.0) /
                (t / 1000.0)
         << " MB/s" << endl;
#endif
  }

  part = RUN_SIZE / num_thread;
  // Run
  Timer sw;
#ifdef LATENCY
  vector<size_t> latency_all;
  Mutex latency_mtx;
#endif
  std::function<void(size_t start, size_t len, int tid)> fun;
  auto operate = [&](size_t start, size_t len, int tid) {
    vector<size_t> latency;
    auto end = start + len;
    Timer l;
#ifdef VIPERT
    auto c = h->get_client();
    for (size_t i = start; i < end; i++) {
#ifdef LATENCY
      l.start();
#endif
      if (ops[i] == OP_INSERT) {
        h->insert(keys[i], value_lens[i], reinterpret_cast<char *>(values), c);
      } else if (ops[i] == OP_UPDATE) {
        h->update(keys[i], value_lens[i], reinterpret_cast<char *>(values), c);
      } else if (ops[i] == OP_READ) {
        uint64_t v;
        auto r = h->find(keys[i], c, &v);
      } else if (ops[i] == OP_DELETE) {
        h->erase(keys[i], c);
      }
#ifdef LATENCY
      latency.push_back(l.elapsed<std::chrono::nanoseconds>());
#endif
    }
    h->load_factor(c);
#else
#ifdef LATENCY
#ifdef HALOT
    l.start();
#endif
#endif
    bool rf = false;
    for (size_t i = start; i < end; i++) {
#ifdef LATENCY
#ifndef HALOT
      l.start();
#endif
#endif
      if (ops[i] == OP_INSERT) {
        rf = h->insert(keys[i], value_lens[i], reinterpret_cast<char *>(values),
                       tid, &r[i % 1024]);
      } else if (ops[i] == OP_UPDATE) {
        rf = h->update(keys[i], value_lens[i], reinterpret_cast<char *>(values),
                       tid, &r[i % 1024]);
      } else if (ops[i] == OP_READ) {
#ifdef HALOT
        rf = h->find(keys[i], &p[i]);
#else
        rf = h->find(keys[i]);
#endif
      } else if (ops[i] == OP_DELETE) {
        h->erase(keys[i], tid);
        rf = true;
      }
#ifdef LATENCY
#ifndef HALOT
      latency.push_back(l.elapsed<std::chrono::nanoseconds>());
#else
      if (rf) {
        latency.push_back(l.elapsed<std::chrono::nanoseconds>());
        rf = false;
        l.start();
      }
#endif

#endif
    }
#endif

#ifdef LATENCY
    lock_guard<Mutex> lock(latency_mtx);
    latency_all.insert(latency_all.end(), latency.begin(), latency.end());
#endif
    h->wait();
  };
  fun = operate;
  thread ths[num_thread];
#ifdef PM_PCM
  auto before_state = getSystemCounterState();
#endif
  sw.start();
  for (size_t i = 0; i < num_thread; i++) {
    ths[i] = thread(fun, part * i, part, i);
  }
  for (size_t i = 0; i < num_thread; i++) {
    ths[i].join();
  }
  auto t = sw.elapsed<std::chrono::milliseconds>();

  printf("Throughput: run, %f Mops/s\n",
         ((RUN_SIZE * 1.0) / 1000000) / (t / 1000));
#ifdef HALOT
  h->load_factor();
#endif

#ifdef PM_PCM
  auto after_sstate = getSystemCounterState();
  cout << "MB ReadFromPMM: "
       << getBytesReadFromPMM(before_state, after_sstate) / 1000000 << " "
       << (getBytesReadFromPMM(before_state, after_sstate) / 1000000.0) /
              (t / 1000.0)
       << " MB/s" << endl;
  cout << "MB WrittenToPMM: "
       << getBytesWrittenToPMM(before_state, after_sstate) / 1000000 << " "
       << (getBytesWrittenToPMM(before_state, after_sstate) / 1000000.0) /
              (t / 1000.0)
       << " MB/s" << endl;
#endif
#ifdef LATENCY
  sort(latency_all.begin(), latency_all.end());
  auto sz = latency_all.size();
  size_t avg = 0;
  for (size_t i = 0; i < sz; i++) {
    avg += latency_all[i];
  }
  avg /= sz;

  cout << "Latency: " << avg << " ns\n";
  cout << "\t0 " << latency_all[0] << "\n"
       << "\t50% " << latency_all[size_t(0.5 * sz)] << "\n"
       << "\t90% " << latency_all[size_t(0.9 * sz)] << "\n"
       << "\t99% " << latency_all[size_t(0.99 * sz)] << "\n"
       << "\t99.9% " << latency_all[size_t(0.999 * sz)] << "\n"
       << "\t99.99% " << latency_all[size_t(0.9999 * sz)] << "\n"
       << "\t99.999% " << latency_all[size_t(0.99999 * sz)] << "\n"
       << "\t100% " << latency_all[sz - 1] << endl;
#endif
  delete[] r;

#ifdef HALOT
  delete[] p;
#endif
}

inline void clear_viper_pmem_files() {
  const std::string viper_pmem_path = "/mnt/pmem/hash/VIPER.data/";

  if (!std::filesystem::exists(viper_pmem_path)) return;

  for (const auto& entry :
       std::filesystem::directory_iterator(viper_pmem_path)) {
    std::filesystem::remove_all(entry.path());
  }
}


static void load_ycsb_keys(const std::string& path,
                           std::vector<uint64_t>& keys_out,
                           size_t limit /*=30000000*/) {
  keys_out.clear();
  std::ifstream in(path);
  std::string line;

  while (keys_out.size() < limit && std::getline(in, line)) {
    // 只关心 INSERT / READ / UPDATE
    if (line.find("INSERT") == std::string::npos &&
        line.find("READ")   == std::string::npos &&
        line.find("UPDATE") == std::string::npos) {
      continue;
    }

    auto pos = line.find("usertable user");
    if (pos == std::string::npos) continue;

    pos += strlen("usertable user");
    auto end = line.find(' ', pos);
    if (end == std::string::npos) end = line.size();

    uint64_t k = std::stoull(line.substr(pos, end - pos));
    keys_out.push_back(k);
  }
}

enum class TestOp {
  INSERT,
  READ,
  READ_NEGATIVE,
  UPDATE,
  DELETE_OP
};

enum class YcsbOp : uint8_t {
  INSERT = 0,
  READ   = 1,
  UPDATE = 2,
};

struct YcsbOpKey {
  YcsbOp op;
  uint64_t key;
};

static void load_ycsb_op_keys(const std::string& path,
                             std::vector<YcsbOpKey>& ops_out,
                             size_t limit /*=30000000*/) {
  ops_out.clear();
  std::ifstream in(path);
  std::string line;

  while (ops_out.size() < limit && std::getline(in, line)) {
    YcsbOp op;

    if (line.find("INSERT") != std::string::npos) {
      op = YcsbOp::INSERT;
    } else if (line.find("READ") != std::string::npos) {
      op = YcsbOp::READ;
    } else if (line.find("UPDATE") != std::string::npos) {
      op = YcsbOp::UPDATE;
    } else {
      continue;
    }

    auto pos = line.find("usertable user");
    if (pos == std::string::npos) continue;

    pos += strlen("usertable user");
    auto end = line.find(' ', pos);
    if (end == std::string::npos) end = line.size();

    uint64_t key = std::stoull(line.substr(pos, end - pos));
    ops_out.push_back({op, key});
  }
}

#ifdef HALOT
// double test_latency_halo(const std::string& load_path,
//                          const std::string& run_path,
//                          TestOp op,
//                          size_t max_keys = 200000000,
//                          size_t neg_reads = 1000000) {
//   /* ---------------- load keys ---------------- */
//   std::vector<uint64_t> load_keys;
//   load_ycsb_keys(load_path, load_keys, max_keys);
//   // if (load_keys.empty()) return 0.0;

//   /* ---------------- run keys (optional) ---------------- */
//   std::vector<uint64_t> run_keys;
//   if (!run_path.empty() &&
//       (op == TestOp::READ || op == TestOp::UPDATE)) {
//     load_ycsb_keys(run_path, run_keys, max_keys);
//   }

//   hash_api* h = new hash_api();
//   h->load_factor();

// #if !defined(HALOT)
//   std::cerr << "HALOT build required\n";
//   delete h;
//   return 0.0;
// #endif

//   char val8[8] = {'T','E','S','T',0,0,0,0};
//   std::vector<uint64_t> inserted_keys;
//   inserted_keys.reserve(load_keys.size());

//   size_t ops = 0;
//   auto t_begin = std::chrono::high_resolution_clock::now();

//   /* --------------------------------------------------
//    * INSERT only
//    * -------------------------------------------------- */
//   if (op == TestOp::INSERT) {
//     auto t0 = std::chrono::high_resolution_clock::now();
//     for (uint64_t k : load_keys) {
//       int r = 0;
//       h->insert(k, 8, val8, 0, &r);
//       ops++;
//     }
//     h->wait();
//     t_begin = t0;
//   }

//   /* --------------------------------------------------
//    * Others: need load phase first
//    * -------------------------------------------------- */
//   else {
//     /* ---- load phase ---- */
//     for (uint64_t k : load_keys) {
//       int r = 0;
//       h->insert(k, 8, val8, 0, &r);
//       if (r == DONE) inserted_keys.push_back(k);
//     }
//     h->wait();

//     /* --------------------------------------------------
//      * READ
//      * -------------------------------------------------- */
//     if (op == TestOp::READ) {
// #ifdef NONVAR
//       Pair_t<size_t, size_t> p;
// #elif VARVALUE
//       Pair_t<size_t, std::string> p;
// #else
//       Pair_t<std::string, std::string> p;
// #endif
//       const std::vector<uint64_t>& read_keys =
//           run_keys.empty() ? inserted_keys : run_keys;

//       auto t0 = std::chrono::high_resolution_clock::now();
//       for (uint64_t k : read_keys) {
//         if(h->find(k, &p))
//           ops++;
//         else{
//           cout<<"read fail: "<<k<<endl;
//           // break;
//         }
//       }
//       h->wait();
//       t_begin = t0;
//     }

//     /* --------------------------------------------------
//      * UPDATE
//      * -------------------------------------------------- */
//     else if (op == TestOp::UPDATE) {
//       const std::vector<uint64_t>& update_keys =
//           run_keys.empty() ? inserted_keys : run_keys;

//       auto t0 = std::chrono::high_resolution_clock::now();
//       for (uint64_t k : update_keys) {
//         uint64_t v = k ^ 0xdeadbeef;
//         char v8[8];
//         std::memcpy(v8, &v, 8);
//         int r = 0;
//         h->update(k, 8, v8, 0, &r);
//         ops++;
//       }
//       h->wait();
//       t_begin = t0;
//     }

//     /* --------------------------------------------------
//      * DELETE
//      * -------------------------------------------------- */
//     else if (op == TestOp::DELETE_OP) {
//       auto t0 = std::chrono::high_resolution_clock::now();
//       for (uint64_t k : inserted_keys) {
//         h->erase(k, 0);
//         ops++;
//       }
//       h->wait();
//       t_begin = t0;
//     }

//     /* --------------------------------------------------
//      * READ NEGATIVE
//      * -------------------------------------------------- */
//     else if (op == TestOp::READ_NEGATIVE) {
// #ifdef NONVAR
//       Pair_t<size_t, size_t> p;
// #elif VARVALUE
//       Pair_t<size_t, std::string> p;
// #else
//       Pair_t<std::string, std::string> p;
// #endif
//       uint64_t neg_key = inserted_keys[0] ^ (1ULL << 63);

//       auto t0 = std::chrono::high_resolution_clock::now();
//       for (size_t i = 0; i < neg_reads; ++i) {
//         h->find(neg_key, &p);
//         ops++;
//       }
//       h->wait();
//       t_begin = t0;
//     }
//   }

//   auto t_end = std::chrono::high_resolution_clock::now();
//   h->load_factor();
//   delete h;

//   if (ops == 0) return 0.0;
//   cout<<"time:"<<std::chrono::duration<double>(t_end - t_begin).count() * 1e6 <<"ops："<<ops<<endl;
//   return std::chrono::duration<double>(t_end - t_begin).count() * 1e6 / ops;
// }

double test_latency_halo(const std::string& load_path,
                         const std::string& run_path,
                         TestOp op,
                         size_t max_keys = 200000000,
                         size_t neg_reads = 1000000) {
  /* ---------------- load keys ---------------- */
  std::vector<uint64_t> load_keys;
  load_ycsb_keys(load_path, load_keys, max_keys);
  if (load_keys.empty()) return 0.0;

  /* ---------------- run keys (optional) ---------------- */
  std::vector<uint64_t> run_keys;
  if (!run_path.empty() &&
      (op == TestOp::READ || op == TestOp::UPDATE)) {
    load_ycsb_keys(run_path, run_keys, max_keys);
  }

  hash_api* h = new hash_api();
  h->load_factor();

#if !defined(HALOT)
  std::cerr << "HALOT build required\n";
  delete h;
  return 0.0;
#endif

  char val8[8] = {'T','E','S','T',0,0,0,0};
  std::vector<uint64_t> inserted_keys;
  inserted_keys.reserve(load_keys.size());

  size_t ops = 0;
  auto t_begin = std::chrono::high_resolution_clock::now();

  // 存储不同装载率下的延迟结果
  std::vector<double> load_factor_latencies;
  std::vector<size_t> load_factor_counts;
  
  // 定义装载率节点 (10%, 20%, ..., 100%)
  std::vector<double> load_factors = {0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0};
  size_t current_factor_idx = 0;

  /* --------------------------------------------------
   * INSERT only
   * -------------------------------------------------- */
  if (op == TestOp::INSERT) {
    auto t0 = std::chrono::high_resolution_clock::now();
    auto last_checkpoint = t0;
    size_t checkpoint_ops = 0;
    
    for (size_t i = 0; i < load_keys.size(); ++i) {
      uint64_t k = load_keys[i];
      int r = 0;
      h->insert(k, 8, val8, 0, &r);
      ops++;
      
      // 检查是否达到下一个装载率节点
      double current_progress = static_cast<double>(i + 1) / load_keys.size();
      if (current_progress >= load_factors[current_factor_idx] && current_factor_idx < load_factors.size()) {
        h->wait(); // 确保当前所有插入完成
        
        auto now = std::chrono::high_resolution_clock::now();
        double batch_time_ms = std::chrono::duration<double, std::milli>(now - last_checkpoint).count();
        double batch_avg_latency = batch_time_ms / (i + 1 - checkpoint_ops);
        
        std::cout << "Load factor " << (load_factors[current_factor_idx] * 100) << "%, "
                  << "Keys: " << (i + 1) << ", "
                  << "Batch avg latency: " << batch_avg_latency << " ms/op" << std::endl;
        
        load_factor_latencies.push_back(batch_avg_latency);
        load_factor_counts.push_back(i + 1 - checkpoint_ops);
        
        last_checkpoint = now;
        checkpoint_ops = i + 1;
        current_factor_idx++;
      }
    }
    
    // 确保最后一个等待
    h->wait();
    t_begin = t0;
  }

  /* --------------------------------------------------
   * Others: need load phase first
   * -------------------------------------------------- */
  else {
    /* ---- load phase ---- */
    std::cout << "Loading data for test..." << std::endl;
    for (uint64_t k : load_keys) {
      int r = 0;
      h->insert(k, 8, val8, 0, &r);
      if (r == DONE) inserted_keys.push_back(k);
    }
    h->wait();
    std::cout << "Load completed. " << inserted_keys.size() << " keys loaded." << std::endl;

    /* --------------------------------------------------
     * READ
     * -------------------------------------------------- */
    if (op == TestOp::READ) {
#ifdef NONVAR
      Pair_t<size_t, size_t> p;
#elif VARVALUE
      Pair_t<size_t, std::string> p;
#else
      Pair_t<std::string, std::string> p;
#endif
      const std::vector<uint64_t>& read_keys =
          run_keys.empty() ? inserted_keys : run_keys;
      
      auto t0 = std::chrono::high_resolution_clock::now();
      auto last_checkpoint = t0;
      size_t checkpoint_ops = 0;
      
      for (size_t i = 0; i < read_keys.size(); ++i) {
        uint64_t k = read_keys[i];
        if(h->find(k, &p))
          ops++;
        else{
          std::cout << "read fail: " << k << std::endl;
        }
        
        // 检查是否达到下一个装载率节点
        double current_progress = static_cast<double>(i + 1) / read_keys.size();
        if (current_progress >= load_factors[current_factor_idx] && current_factor_idx < load_factors.size()) {
          h->wait(); // 确保当前所有操作完成
          
          auto now = std::chrono::high_resolution_clock::now();
          double batch_time_ms = std::chrono::duration<double, std::milli>(now - last_checkpoint).count();
          double batch_avg_latency = batch_time_ms / (i + 1 - checkpoint_ops);
          
          std::cout << "Progress " << (load_factors[current_factor_idx] * 100) << "%, "
                    << "Operations: " << (i + 1) << ", "
                    << "Batch avg latency: " << batch_avg_latency << " ms/op" << std::endl;
          
          load_factor_latencies.push_back(batch_avg_latency);
          load_factor_counts.push_back(i + 1 - checkpoint_ops);
          
          last_checkpoint = now;
          checkpoint_ops = i + 1;
          current_factor_idx++;
        }
      }
      h->wait();
      t_begin = t0;
    }

    /* --------------------------------------------------
     * UPDATE
     * -------------------------------------------------- */
    else if (op == TestOp::UPDATE) {
      const std::vector<uint64_t>& update_keys =
          run_keys.empty() ? inserted_keys : run_keys;
      
      auto t0 = std::chrono::high_resolution_clock::now();
      auto last_checkpoint = t0;
      size_t checkpoint_ops = 0;
      
      for (size_t i = 0; i < update_keys.size(); ++i) {
        uint64_t k = update_keys[i];
        uint64_t v = k ^ 0xdeadbeef;
        char v8[8];
        std::memcpy(v8, &v, 8);
        int r = 0;
        h->update(k, 8, v8, 0, &r);
        ops++;
        
        // 检查是否达到下一个装载率节点
        double current_progress = static_cast<double>(i + 1) / update_keys.size();
        if (current_progress >= load_factors[current_factor_idx] && current_factor_idx < load_factors.size()) {
          h->wait(); // 确保当前所有操作完成
          
          auto now = std::chrono::high_resolution_clock::now();
          double batch_time_ms = std::chrono::duration<double, std::milli>(now - last_checkpoint).count();
          double batch_avg_latency = batch_time_ms / (i + 1 - checkpoint_ops);
          
          std::cout << "Progress " << (load_factors[current_factor_idx] * 100) << "%, "
                    << "Operations: " << (i + 1) << ", "
                    << "Batch avg latency: " << batch_avg_latency << " ms/op" << std::endl;
          
          load_factor_latencies.push_back(batch_avg_latency);
          load_factor_counts.push_back(i + 1 - checkpoint_ops);
          
          last_checkpoint = now;
          checkpoint_ops = i + 1;
          current_factor_idx++;
        }
      }
      h->wait();
      t_begin = t0;
    }

    /* --------------------------------------------------
     * DELETE
     * -------------------------------------------------- */
    else if (op == TestOp::DELETE_OP) {
      auto t0 = std::chrono::high_resolution_clock::now();
      auto last_checkpoint = t0;
      size_t checkpoint_ops = 0;
      
      for (size_t i = 0; i < inserted_keys.size(); ++i) {
        uint64_t k = inserted_keys[i];
        h->erase(k, 0);
        ops++;
        
        // 检查是否达到下一个装载率节点
        double current_progress = static_cast<double>(i + 1) / inserted_keys.size();
        if (current_progress >= load_factors[current_factor_idx] && current_factor_idx < load_factors.size()) {
          h->wait(); // 确保当前所有操作完成
          
          auto now = std::chrono::high_resolution_clock::now();
          double batch_time_ms = std::chrono::duration<double, std::milli>(now - last_checkpoint).count();
          double batch_avg_latency = batch_time_ms / (i + 1 - checkpoint_ops);
          
          std::cout << "Progress " << (load_factors[current_factor_idx] * 100) << "%, "
                    << "Operations: " << (i + 1) << ", "
                    << "Batch avg latency: " << batch_avg_latency << " ms/op" << std::endl;
          
          load_factor_latencies.push_back(batch_avg_latency);
          load_factor_counts.push_back(i + 1 - checkpoint_ops);
          
          last_checkpoint = now;
          checkpoint_ops = i + 1;
          current_factor_idx++;
        }
      }
      h->wait();
      t_begin = t0;
    }

    /* --------------------------------------------------
     * READ NEGATIVE
     * -------------------------------------------------- */
    else if (op == TestOp::READ_NEGATIVE) {
#ifdef NONVAR
      Pair_t<size_t, size_t> p;
#elif VARVALUE
      Pair_t<size_t, std::string> p;
#else
      Pair_t<std::string, std::string> p;
#endif
      uint64_t neg_key = inserted_keys[0] ^ (1ULL << 63);
      
      auto t0 = std::chrono::high_resolution_clock::now();
      auto last_checkpoint = t0;
      size_t checkpoint_ops = 0;
      
      for (size_t i = 0; i < neg_reads; ++i) {
        h->find(neg_key, &p);
        ops++;
        
        // 检查是否达到下一个装载率节点
        double current_progress = static_cast<double>(i + 1) / neg_reads;
        if (current_progress >= load_factors[current_factor_idx] && current_factor_idx < load_factors.size()) {
          h->wait(); // 确保当前所有操作完成
          
          auto now = std::chrono::high_resolution_clock::now();
          double batch_time_ms = std::chrono::duration<double, std::milli>(now - last_checkpoint).count();
          double batch_avg_latency = batch_time_ms / (i + 1 - checkpoint_ops);
          
          std::cout << "Progress " << (load_factors[current_factor_idx] * 100) << "%, "
                    << "Operations: " << (i + 1) << ", "
                    << "Batch avg latency: " << batch_avg_latency << " ms/op" << std::endl;
          
          load_factor_latencies.push_back(batch_avg_latency);
          load_factor_counts.push_back(i + 1 - checkpoint_ops);
          
          last_checkpoint = now;
          checkpoint_ops = i + 1;
          current_factor_idx++;
        }
      }
      h->wait();
      t_begin = t0;
    }
  }

  auto t_end = std::chrono::high_resolution_clock::now();
  h->load_factor();
  delete h;

  if (ops == 0) return 0.0;
  
  double final_latency = std::chrono::duration<double>(t_end - t_begin).count() * 1e6 / ops;
  
  // 输出装载率延迟总结
  std::cout << "\n=== Load Factor Latency Summary ===" << std::endl;
  for (size_t i = 0; i < load_factor_latencies.size(); ++i) {
    std::cout << "At " << (load_factors[i] * 100) << "%, "
              << "Operations: " << load_factor_counts[i] << ", "
              << "Avg Latency: " << load_factor_latencies[i] << " ms/op" << std::endl;
  }
  
  std::cout << "\nFinal average latency: " << final_latency << " us/op" << std::endl;
  
  return final_latency;
}

double test_halo_mt_throughput_ycsb(const std::string& load_path,
                                    const std::string& run_path,
                                    int thread_num,
                                    size_t load_limit,
                                    size_t run_limit) {
  /* ---------------- load phase ---------------- */
  std::vector<uint64_t> load_keys;
  load_ycsb_keys(load_path, load_keys, load_limit);
  hash_api h;
  char val8[8] = {'T','E','S','T',0,0,0,0};
  char val256[VAL_LEN];
  memset(val256, '1', VAL_LEN);

  for (uint64_t k : load_keys) {
    int r = 0;
    h.insert(k, VAL_LEN, val256, 0, &r);
  }
  h.wait();
  h.load_factor();

  /* ---------------- run phase ---------------- */
  std::vector<YcsbOpKey> run_ops;
  load_ycsb_op_keys(run_path, run_ops, run_limit);

  // std::atomic<bool> start_flag{false};
  // std::atomic<size_t> global_ops{0};

  size_t total_ops = run_ops.size();
  size_t per = total_ops / thread_num;

  std::vector<std::thread> threads;
  threads.reserve(thread_num);

  auto t_begin = std::chrono::high_resolution_clock::now();

  for (int tid = 0; tid < thread_num; tid++) {
    size_t b = tid * per;
    size_t e = (tid == thread_num - 1) ? total_ops : (b + per);

    threads.emplace_back([&, tid, b, e]() {
#ifdef NONVAR
      Pair_t<size_t, size_t> p;
#elif VARVALUE
      Pair_t<size_t, std::string> p;
#else
      Pair_t<std::string, std::string> p;
#endif
      char v8[8];

      // barrier
      // while (!start_flag.load(std::memory_order_acquire)) {
      //   _mm_pause();
      // }

      for (size_t i = b; i < e; i++) {
        const auto& ok = run_ops[i];
        int r = 0;

        switch (ok.op) {
          case YcsbOp::READ:
            h.find(ok.key, &p);
            break;

          case YcsbOp::INSERT:
            h.insert(ok.key, VAL_LEN, val256, tid, &r);
            break;

          case YcsbOp::UPDATE: {
            uint64_t v = ok.key ^ 0xdeadbeef;
            memcpy(v8, &v, 8);
            h.update(ok.key, 8, v8, tid, &r);
            break;
          }
        }

        //global_ops.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }

  // start all threads together
  // start_flag.store(true, std::memory_order_release);

  for (auto& th : threads) th.join();
  h.wait();
  h.load_factor();
  auto t_end = std::chrono::high_resolution_clock::now();

  double time_s =
      std::chrono::duration<double>(t_end - t_begin).count();
  double mops =
      (double)run_limit / 1e6 / time_s; //global_ops.load()

  std::cout << "[HALO YCSB MT] threads=" << thread_num
            << " ops=" << run_limit//global_ops
            << " time=" << time_s
            << " throughput=" << mops << " Mops/s\n";

  return mops;
}
#endif

/*--------------------------------------------------
 *  Viper 统一延迟测试函数（单线程）
 *  接口与 test_latency_halo 完全一致
 *------------------------------------------------*/
#ifdef VIPERT
double test_latency_viper(const std::string& load_path,
                          const std::string& run_path,
                          TestOp op,
                          size_t max_keys = 200000000,
                          size_t neg_reads = 1000000) {
  /* ---------------- load keys ---------------- */
  std::vector<uint64_t> load_keys;
  load_ycsb_keys(load_path, load_keys, max_keys);
  if (load_keys.empty()) return 0.0;

  /* ---------------- run keys (optional) ---------------- */
  std::vector<uint64_t> run_keys;
  if (!run_path.empty() &&
      (op == TestOp::READ || op == TestOp::UPDATE)) {
    load_ycsb_keys(run_path, run_keys, max_keys);
  }

#if !defined(VIPERT)
  std::cerr << "VIPERT build required\n";
  return 0.0;
#endif

  hash_api* h = new hash_api();
  {
    auto client = h->get_client();

    char val8[8] = {'T','E','S','T',0,0,0,0};
    std::vector<uint64_t> inserted_keys;
    inserted_keys.reserve(load_keys.size());

    size_t ops = 0;
    auto t_begin = std::chrono::high_resolution_clock::now();

    /* --------------------------------------------------
    * INSERT only
    * -------------------------------------------------- */
    if (op == TestOp::INSERT) {
      auto t0 = std::chrono::high_resolution_clock::now();
      for (uint64_t k : load_keys) {
        h->insert(k, 8, val8, client);
        ops++;
      }
      t_begin = t0;
    }

    /* --------------------------------------------------
    * Others: need load phase first
    * -------------------------------------------------- */
    else {
      /* ---- load phase (not timed) ---- */
      for (uint64_t k : load_keys) {
        h->insert(k, 8, val8, client);
        inserted_keys.push_back(k);
      }

      /* --------------------------------------------------
      * READ
      * -------------------------------------------------- */
      if (op == TestOp::READ) {
        const std::vector<uint64_t>& read_keys =
            run_keys.empty() ? inserted_keys : run_keys;

        auto t0 = std::chrono::high_resolution_clock::now();
        for (uint64_t k : read_keys) {
          uint64_t v;
          h->find(k, client, &v);
          ops++;
        }
        t_begin = t0;
      }

      /* --------------------------------------------------
      * UPDATE
      * -------------------------------------------------- */
      else if (op == TestOp::UPDATE) {
        const std::vector<uint64_t>& update_keys =
            run_keys.empty() ? inserted_keys : run_keys;

        auto t0 = std::chrono::high_resolution_clock::now();
        for (uint64_t k : update_keys) {
          uint64_t v = k ^ 0xdeadbeef;
          char v8[8];
          std::memcpy(v8, &v, 8);
          h->update(k, 8, v8, client);
          ops++;
        }
        t_begin = t0;
      }

      /* --------------------------------------------------
      * DELETE
      * -------------------------------------------------- */
      else if (op == TestOp::DELETE_OP) {
        auto t0 = std::chrono::high_resolution_clock::now();
        for (uint64_t k : inserted_keys) {
          h->erase(k, client);
          ops++;
        }
        t_begin = t0;
      }

      /* --------------------------------------------------
      * READ NEGATIVE
      * -------------------------------------------------- */
      else if (op == TestOp::READ_NEGATIVE) {
        uint64_t neg_key = inserted_keys[0] ^ (1ULL << 63);

        auto t0 = std::chrono::high_resolution_clock::now();
        for (size_t i = 0; i < neg_reads; ++i) {
          uint64_t v;
          h->find(neg_key, client, &v);
          ops++;
        }
        t_begin = t0;
      }
    }

    auto t_end = std::chrono::high_resolution_clock::now();

    h->load_factor(client);
    if (ops == 0) return 0.0;

    std::cout << "time: "
              << std::chrono::duration<double>(t_end - t_begin).count() * 1e6
              << " us, ops: " << ops << std::endl;

    return std::chrono::duration<double>(t_end - t_begin).count() * 1e6 / ops;
  }
  delete h;
}

#endif

#ifdef VIPERT
double test_viper_mt_throughput_ycsb(const std::string& load_path,
                                     const std::string& run_path,
                                     int thread_num,
                                     size_t load_limit,
                                     size_t run_limit) {
  /* ---------------- load phase ---------------- */
  std::vector<uint64_t> load_keys;
  load_ycsb_keys(load_path, load_keys, load_limit);

  hash_api h;

  // 单 client 进行 load（不计时）
  auto load_client = h.get_client();
  char val8[8] = {'T','E','S','T',0,0,0,0};
  char val256[VAL_LEN];
  memset(val256, '1', VAL_LEN);
  for (uint64_t k : load_keys) {
    h.insert(k, 8, val8, load_client);
  }

  h.load_factor(load_client);

  /* ---------------- run phase ---------------- */
  std::vector<YcsbOpKey> run_ops;
  load_ycsb_op_keys(run_path, run_ops, run_limit);

  // std::atomic<bool> start_flag{false};
  // std::atomic<size_t> global_ops{0};

  size_t total_ops = run_ops.size();
  size_t per = total_ops / thread_num;

  std::vector<std::thread> threads;
  threads.reserve(thread_num);

  auto t_begin = std::chrono::high_resolution_clock::now();

  for (int tid = 0; tid < thread_num; tid++) {
    size_t b = tid * per;
    size_t e = (tid == thread_num - 1) ? total_ops : (b + per);

    threads.emplace_back([&, b, e]() {
      // 每个线程一个 client（✅ VIPER 语义）
      auto client = h.get_client();

      char v8[8];

      // barrier
      // while (!start_flag.load(std::memory_order_acquire)) {
      //   _mm_pause();
      // }

      for (size_t i = b; i < e; i++) {
        const auto& ok = run_ops[i];

        switch (ok.op) {
          case YcsbOp::READ: {
            uint64_t v;
            h.find(ok.key, client, &v);
            break;
          }

          case YcsbOp::INSERT: {
            h.insert(ok.key, VAL_LEN, val256, client);
            break;
          }

          case YcsbOp::UPDATE: {
            uint64_t v = ok.key ^ 0xdeadbeef;
            std::memcpy(v8, &v, 8);
            h.update(ok.key, 8, v8, client);
            break;
          }

          default:
            break;
        }

        //global_ops.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }

  // start all threads together
  //start_flag.store(true, std::memory_order_release);

  for (auto& th : threads) th.join();

  auto t_end = std::chrono::high_resolution_clock::now();
  clear_viper_pmem_files();
  double time_s =
      std::chrono::duration<double>(t_end - t_begin).count();
  double mops =
      (double)total_ops / 1e6 / time_s;

  std::cout << "[VIPER YCSB MT] threads=" << thread_num
            << " ops=" << total_ops
            << " time=" << time_s
            << " throughput=" << mops << " Mops/s\n";

  return mops;
}

enum class TestOperation {
    INSERT,
    READ,
    UPDATE,
    DELETE_OP
};

std::string TestOpToString(TestOperation op) {
    switch (op) {
        case TestOperation::INSERT: return "INSERT";
        case TestOperation::READ: return "READ";
        case TestOperation::UPDATE: return "UPDATE";
        case TestOperation::DELETE_OP: return "DELETE";
        default: return "UNKNOWN";
    }
}

double test_viper_load_factor_performance(
    const std::string& load_path,
    TestOperation op,
    size_t total_slots = 2000000,    // 总槽位数
    bool disable_resize = true,    // 禁用扩容
    int num_load_factors = 9       // 测试9个装载率 (10%, 20%, ..., 90%)
) {
    if (total_slots <= 0) {
        std::cerr << "Error: total_slots must be positive" << std::endl;
        return 0.0;
    }
    
    /* ---------------- 加载键（可能需要比总槽位数更多的键） ---------------- */
    std::vector<uint64_t> available_keys;
    load_ycsb_keys(load_path, available_keys, total_slots * 2);  // 加载双倍的键，确保足够
    
    if (available_keys.size() < total_slots) {
        std::cerr << "Error: Not enough keys in load file. Need at least " 
                  << total_slots << " keys, but only got " << available_keys.size() << std::endl;
        return 0.0;
    }
    
    std::cout << "Available keys: " << available_keys.size() << std::endl;
    
    /* ---------------- 创建固定容量的Viper实例 ---------------- */
    viper::ViperConfig config;
    config.resize_threshold = disable_resize ? 1.0 : 0.85;  // 如果禁用扩容，设置阈值为1.0
    config.enable_reclamation = false;  // 禁用垃圾回收
    
    // 计算所需空间（确保足够存储total_slots个键值对）
    size_t slots_per_page = viper::internal::get_num_slots_per_page<uint64_t, uint64_t>();
    size_t pages_per_block = viper::BLOCK_SIZE / sizeof(viper::internal::ViperPage<uint64_t, uint64_t>);
    size_t slots_per_block = pages_per_block * slots_per_page;
    size_t blocks_needed = (total_slots + slots_per_block - 1) / slots_per_block;
    
    // 增加10%的安全余量
    size_t estimated_blocks = blocks_needed * 110 / 100;
    size_t pool_size = viper::PAGE_SIZE + estimated_blocks * viper::BLOCK_SIZE;
    
    // 对齐到1GB
    pool_size = ((pool_size + viper::ONE_GB - 1) / viper::ONE_GB) * viper::ONE_GB;
    
    std::cout << "Creating fixed-capacity Viper:" << std::endl;
    std::cout << "  Total slots: " << total_slots << std::endl;
    std::cout << "  Slots per page: " << slots_per_page << std::endl;
    std::cout << "  Pages per block: " << pages_per_block << std::endl;
    std::cout << "  Blocks needed: " << blocks_needed << " (+10% buffer = " << estimated_blocks << ")" << std::endl;
    std::cout << "  Pool size: " << pool_size / (1024.0 * 1024 * 1024) << " GB" << std::endl;
    
    // 创建Viper实例
    auto viper = viper::Viper<uint64_t, uint64_t>::create(
        "/mnt/pmem/viper-load-factor-test.pool", pool_size, config);
    
    // 结果存储
    std::vector<double> load_factor_results(num_load_factors, 0.0);
    std::vector<double> avg_latency_results(num_load_factors, 0.0);
    std::vector<size_t> keys_per_factor(num_load_factors, 0);
    
    /* ---------------- 测试不同装载率 ---------------- */
    for (int factor_idx = 0; factor_idx < num_load_factors; ++factor_idx) {
        double load_factor = (factor_idx + 1) * 0.1;  // 10%, 20%, ..., 90%
        size_t target_keys = static_cast<size_t>(total_slots * load_factor);
        
        std::cout << "\n\n=== Testing Load Factor: " << (load_factor * 100) << "% (" 
                  << target_keys << " keys) ===" << std::endl;
        
        // 使用新的客户端（确保每个测试点是独立的）
        auto client = viper->get_client();
        
        /* ---- 阶段1: 加载数据到目标装载率 ---- */
        std::cout << "Phase 1: Loading data..." << std::endl;
        
        // 确定要使用的键
        std::vector<uint64_t> keys_to_use(available_keys.begin(), 
                                         available_keys.begin() + target_keys);
        std::vector<uint64_t> loaded_keys;
        loaded_keys.reserve(target_keys);
        
        uint64_t test_value = 0x123456789ABCDEF0ULL;
        size_t load_success_count = 0;
        auto load_start = std::chrono::high_resolution_clock::now();
        
        for (size_t i = 0; i < keys_to_use.size(); ++i) {
            bool success = client.put(keys_to_use[i], test_value);
            if (success) {
                loaded_keys.push_back(keys_to_use[i]);
                load_success_count++;
            }
        }
        
        auto load_end = std::chrono::high_resolution_clock::now();
        double load_time_ms = std::chrono::duration<double, std::milli>(load_end - load_start).count();
        
        std::cout << "  Loaded " << load_success_count << "/" << target_keys 
                  << " keys in " << load_time_ms << " ms" << std::endl;
        std::cout << "  Load avg latency: " << (load_time_ms / load_success_count) << " ms/op" << std::endl;
        
        if (load_success_count == 0) {
            std::cerr << "  ERROR: No keys loaded!" << std::endl;
            continue;
        }
        
        keys_per_factor[factor_idx] = load_success_count;
        
        /* ---- 阶段2: 执行测试操作 ---- */
        std::cout << "Phase 2: Testing " << TestOpToString(op) << " operations..." << std::endl;
        
        size_t test_ops = 0;
        double total_latency_ns = 0.0;
        
        // 确定要测试的键（随机选择，避免缓存局部性偏差）
        std::vector<uint64_t> test_keys;
        if (op == TestOperation::READ || op == TestOperation::UPDATE || op == TestOperation::DELETE_OP) {
            // 对已加载的键执行操作
            test_keys = loaded_keys;
        } else if (op == TestOperation::INSERT) {
            // INSERT测试需要新的键，使用剩余的可用键
            size_t start_idx = target_keys;
            size_t end_idx = std::min(available_keys.size(), start_idx + 1000);  // 测试1000次插入
            for (size_t i = start_idx; i < end_idx; ++i) {
                test_keys.push_back(available_keys[i]);
            }
        }
        
        if (test_keys.empty()) {
            std::cerr << "  ERROR: No keys to test!" << std::endl;
            continue;
        }
        
        auto test_start = std::chrono::high_resolution_clock::now();
        
        if (op == TestOperation::READ) {
            for (size_t i = 0; i < test_keys.size(); ++i) {
                uint64_t key = 0;
                uint64_t value;
                
                auto op_start = std::chrono::high_resolution_clock::now();
                bool found = client.get(key, &value);
                auto op_end = std::chrono::high_resolution_clock::now();
                
                // if (found) {
                    test_ops++;
                    total_latency_ns += std::chrono::duration<double, std::nano>(op_end - op_start).count();
                // }
            }
        } 
        else if (op == TestOperation::UPDATE) {
            for (size_t i = 0; i < test_keys.size(); ++i) {
                uint64_t key = test_keys[i];
                uint64_t new_value = key ^ 0xdeadbeefULL;
                
                auto op_start = std::chrono::high_resolution_clock::now();
                bool success = client.put(key, new_value);
                auto op_end = std::chrono::high_resolution_clock::now();
                
                if (success) {
                    test_ops++;
                    total_latency_ns += std::chrono::duration<double, std::nano>(op_end - op_start).count();
                }
            }
        } 
        else if (op == TestOperation::INSERT) {
            // 测试插入新键（这些键不在已加载的键中）
            for (size_t i = 0; i < test_keys.size(); ++i) {
                uint64_t key = test_keys[i];
                uint64_t value = key ^ 0x12345678ULL;
                
                auto op_start = std::chrono::high_resolution_clock::now();
                bool success = client.put(key, value);
                auto op_end = std::chrono::high_resolution_clock::now();
                
                if (success) {
                    test_ops++;
                    total_latency_ns += std::chrono::duration<double, std::nano>(op_end - op_start).count();
                }
            }
        } 
        else if (op == TestOperation::DELETE_OP) {
            // 注意：删除操作会改变装载率，所以我们只在测试完成后删除
            // 或者我们可以先备份已加载的键，然后测试删除
            for (size_t i = 0; i < test_keys.size(); ++i) {
                uint64_t key = test_keys[i];
                
                auto op_start = std::chrono::high_resolution_clock::now();
                bool success = client.remove(key);
                auto op_end = std::chrono::high_resolution_clock::now();
                
                if (success) {
                    test_ops++;
                    total_latency_ns += std::chrono::duration<double, std::nano>(op_end - op_start).count();
                }
            }
        }
        
        auto test_end = std::chrono::high_resolution_clock::now();
        double test_time_ms = std::chrono::duration<double, std::milli>(test_end - test_start).count();
        
        if (test_ops == 0) {
            std::cerr << "  ERROR: No operations completed!" << std::endl;
            avg_latency_results[factor_idx] = 0.0;
            continue;
        }
        
        double avg_latency_ns = total_latency_ns / test_ops;
        double avg_latency_us = avg_latency_ns / 1000.0;
        
        load_factor_results[factor_idx] = load_factor;
        avg_latency_results[factor_idx] = avg_latency_us;
        
        std::cout << "  Operations: " << test_ops << std::endl;
        std::cout << "  Test time: " << test_time_ms << " ms" << std::endl;
        std::cout << "  Average latency: " << avg_latency_us << " us/op" << std::endl;
        
        // 显示当前Viper状态
        viper->print_usage();
    }
    
    /* ---------------- 输出汇总结果 ---------------- */
    std::cout << "\n\n=== LOAD FACTOR PERFORMANCE SUMMARY ===" << std::endl;
    std::cout << "Operation: " << TestOpToString(op) << std::endl;
    std::cout << "Total slots: " << total_slots << std::endl;
    std::cout << "Disabled resize: " << (disable_resize ? "Yes" : "No") << std::endl;
    std::cout << "\nResults:" << std::endl;
    std::cout << "Load Factor | Keys Loaded | Avg Latency (us)" << std::endl;
    std::cout << "------------|-------------|-----------------" << std::endl;
    
    double overall_avg_latency = 0.0;
    size_t valid_results = 0;
    
    for (int i = 0; i < num_load_factors; ++i) {
        if (avg_latency_results[i] > 0.0) {
            std::cout << std::fixed << std::setprecision(1);
            std::cout << std::setw(11) << (load_factor_results[i] * 100) << "% | ";
            std::cout << std::setw(11) << keys_per_factor[i] << " | ";
            std::cout << std::setw(15) << std::setprecision(3) << avg_latency_results[i] << std::endl;
            
            overall_avg_latency += avg_latency_results[i];
            valid_results++;
        }
    }
    
    if (valid_results > 0) {
        overall_avg_latency /= valid_results;
        std::cout << "\nOverall average latency: " << overall_avg_latency << " us/op" << std::endl;
    }
    
    return overall_avg_latency;
}

#endif

int main(int argc, char** argv) {

  const std::string load_path = "../../ycsb_data/basic_op/load2M.txt";
  const std::string load_path_skew = "../../ycsb_data/basic_op/run200M_op.txt";
  // hash_api* h = new hash_api();
  // h->load_factor();
  //double ins = test_latency_viper(load_path, "", TestOp::INSERT,20000000);
  //double rd  = test_latency_viper(load_path, load_path_skew, TestOp::READ, 400000);
  // double upd = test_latency_viper(load_path, load_path_skew, TestOp::UPDATE);
  // double del = test_latency_viper(load_path, "", TestOp::DELETE_OP);
 // double neg = test_latency_viper(load_path, "" , TestOp::READ_NEGATIVE);

  //double ins = test_latency_halo(load_path, "", TestOp::INSERT,40000000);
  //double rd  = test_latency_halo(load_path, load_path_skew, TestOp::READ,2000000);
  //double upd = test_latency_halo(load_path, load_path_skew, TestOp::UPDATE);
  //double del = test_latency_halo(load_path, "", TestOp::DELETE_OP);
  //double neg = test_latency_halo(load_path, "" , TestOp::READ_NEGATIVE);

  // std::cout << "INSERT avg us/op = " << ins << "\n";
  //std::cout << "READ   avg us/op = " << rd  << "\n";
  //std::cout << "UPDATE avg us/op = " << upd << "\n";
  //std::cout << "DELETE avg us/op = " << del << "\n";
  //std::cout << "NEG RD avg us/op = " << neg << "\n";

  #ifdef HALOT
  for (int t : {16}) {
    test_halo_mt_throughput_ycsb(
        "",
        load_path,
        t,
        2000000,   // load
        2000000    // run
    );
  }
  #endif

  #ifdef VIPERT
    for (int t : {16}) {
      test_viper_mt_throughput_ycsb(
          "",
          load_path,
          t,
          2000000,   // load
          2000000    // run
      );
    }
  #endif

    // test_viper_load_factor_performance(
    //     load_path,
    //     TestOperation::DELETE_OP,
    //     2000000,
    //     true
    // );

  return 0;
}


// int main(int argc, char **argv) {
//   bool recovery = false;
//   printf("workload: %s, threads: %s\n", argv[1], argv[2]);
//   // if (argc == 4) recovery = true;
//   // if (recovery) {
//   //   hash_api *h;
//   //   Timer tr;
//   //   tr.start();
//   //   h = new hash_api();
//   //   printf("hash: %s Recovery cost %.1f ms.\n", h->hash_name().c_str(),
//   //          tr.elapsed<std::chrono::milliseconds>());
//   //   delete h;
//   //   return 0;
//   // }
//   string workload = argv[1];
//   workload_type wlt;
//   string load_data = "";
//   string run_data = "";
//   if (workload.find("ycsb") != string::npos) {
//     load_data = "/home/hdk/Halo/YCSB/workloads/ycsb_load_workload";
//     load_data += workload[workload.size() - 1];
//     run_data = "/home/hdk/Halo/YCSB/workloads/ycsb_run_workload";
//     run_data += workload[workload.size() - 1];
//     wlt = YCSB;
//   } else if (workload.find("PiBench") != string::npos) {
//     load_data = "PiBench/";
//     load_data += workload;
//     load_data += ".load";
//     run_data = "PiBench/";
//     run_data += workload;
//     run_data += ".run";
//     wlt = PiBench;
//   }
//   int num_thread = atoi(argv[2]);
//   hash_api *h;
//   run_test(wlt, num_thread, load_data, run_data, h, workload);
//   // auto pid = getpid();
//   // std::array<char, 128> buffer;
//   // std::unique_ptr<FILE, decltype(&pclose)> pipe(
//   //     popen(("cat /proc/" + to_string(pid) + "/status").c_str(), "r"),
//   //     pclose);
//   // if (!pipe) {
//   //   throw std::runtime_error("popen() failed!");
//   // }
//   // while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
//   //   string result = buffer.data();
//   //   if (result.find("VmRSS") != string::npos) {
//   //     std::string mem_ocp = std::regex_replace(
//   //         result, std::regex("[^0-9]*([0-9]+).*"), std::string("$1"));
//   //     printf("DRAM consumption: %.1f GB.\n", stof(mem_ocp) / 1024 / 1024);
//   //     break;
//   //   }
//   // }
// #ifdef SOFTT
//   vmem_stats_print(vmp1, "");
// #elif PCLHTT
//   vmem_stats_print(PCLHT::vmp, "");
// #endif
//   delete h;
//   return 0;
// }
