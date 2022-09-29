#pragma once

#include <unordered_map>
#include <vector>
#include <string>
#include <memory>
#include <thread>
#include <mutex>

#include "const_and_share_struct.h"
#include "random_str.h"

namespace cmp_mem_engine
{

class ShareData
{
private:
    std::unordered_map<HeapKey, CombinedVal> key_vals_;
    // lists of cold(head) -> warm(tail)
    std::list<const std::string*> protected_list_;
    std::list<const std::string*> probationary_list_;

    mutable std::mutex mutex_;

public:
    ShareData() = delete;
    ShareData& operator=(const ShareData& copy) = delete;

    /* install at most init_key_num to key_vals_, 
     * and sample at most sample_key_num keys to samples
     * which will partly go to each thread */
    explicit ShareData(const size_t init_key_num, const size_t sample_key_num, std::vector<std::string>& samples);

    std::string* find_val(const std::string& key);
    float hash_table_load_factor() const;
    float max_hash_table_load_factor() const;
};

class Multi
{
public:
    Multi() = delete;
    Multi& operator=(Multi& copy) = delete;

    explicit Multi(std::shared_ptr<ShareData> data, const std::vector<std::string> samples);
    ~Multi() noexcept;
         
    void start_bench_in_thread(const size_t num);
    void wait_until_thread_finish();

    std::chrono::milliseconds duration() const;
    std::tuple<std::chrono::high_resolution_clock::time_point, std::chrono::high_resolution_clock::time_point>
    get_time_points() const;
    int miss_percent() const;

private:
    void benchmark(const size_t num);

private:
    RandomEngine re_;
    std::shared_ptr<ShareData> data_;
    std::thread thread_;
    const std::vector<std::string>& samples_;
    std::vector<std::string> rand_keys_;

    std::chrono::high_resolution_clock::time_point time_start_;
    std::chrono::high_resolution_clock::time_point time_end_;

    size_t hit_cnt_;
    size_t miss_cnt_;
};

}   // namespace cmp_mem_engine