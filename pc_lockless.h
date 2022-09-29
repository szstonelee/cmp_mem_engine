#pragma once

#include <string>
#include <thread>

#include "const_and_share_struct.h"

namespace cmp_mem_engine
{

// Every producer (thread) have one Task Array
struct LocklessTasks
{
    LocklessTasks()
    {
        for (size_t i = 0; i != kLockLessArrayNum; ++i)
        {
            request_keys[i].store(nullptr);
            result_vals[i].store(nullptr);
        }
    }

    alignas(hardware_destructive_interference_size) std::atomic<const std::string*> request_keys[kLockLessArrayNum];
    alignas(hardware_destructive_interference_size) std::atomic<const std::string*> result_vals[kLockLessArrayNum];
};

class ProducerLockless
{
private:
    RandomEngine re_;
    std::thread thread_;

    LocklessTasks& tasks_;

    std::vector<std::string> hot_keys_;
    std::vector<std::string> random_keys_;

    size_t hit_cnt_ = 0;
    size_t miss_cnt_ = 0;
    std::chrono::high_resolution_clock::time_point time_start_;
    std::chrono::high_resolution_clock::time_point time_end_;

    size_t batch_request_wait_cnt_ = 0;
    size_t batch_request_most_ = 0;
    size_t batch_ressult_wait_cnt_ = 0;
    size_t batch_result_most_ = 0;
    size_t bench_cnt_ = 0;

public:
    ProducerLockless() = delete;
    ProducerLockless(const ProducerLockless&) = delete;
    ProducerLockless(ProducerLockless&&) = delete;
    ProducerLockless& operator=(const ProducerLockless&) = delete;
    ProducerLockless& operator=(ProducerLockless&&) = delete;

    explicit ProducerLockless(const size_t pid, LocklessTasks& tasks, const std::vector<std::string>& samples);
    ~ProducerLockless() noexcept;

    int miss_percent() const;

    void start_thread();
    void wait_until_join();    

    size_t get_bench_count() const;
    std::tuple<std::chrono::high_resolution_clock::time_point, std::chrono::high_resolution_clock::time_point>
    get_time_points() const;
    std::tuple<size_t, size_t, size_t, size_t> get_batch_stats() const;

    void debug_address_of_tasks() const;

private:
    void benchmark();
    std::vector<const std::string*> prepare_input_keys(const size_t num);
    void batch_keys(std::vector<const std::string*>& keys, const size_t debug_loop_no);

    size_t fill_requests(std::vector<const std::string*>& keys, 
                         std::array<bool, kLockLessArrayNum>& is_processing,
                         std::array<const std::string*, kLockLessArrayNum>& processing_keys);
    size_t avail_slot_in_requests(const std::array<bool, kLockLessArrayNum>& is_processing, const size_t start_slot) const;
    size_t get_results(std::array<bool, kLockLessArrayNum>& is_processing,
                       std::array<const std::string*, kLockLessArrayNum>& processing_keys);
};

class ConsumerLockless
{
private:
    std::thread thread_;

    SingleData& cache_;
    std::array<LocklessTasks, kRunProducerNum>& producer_tasks_;

    size_t batch_cnt_ = 0;
    size_t wait_cnt_ = 0;

public:
    ConsumerLockless() = delete;
    ConsumerLockless(const ConsumerLockless&) = delete;
    ConsumerLockless(ConsumerLockless&&) = delete;
    ConsumerLockless& operator=(const ConsumerLockless&) = delete;
    ConsumerLockless& operator=(ConsumerLockless&&) = delete;

    ~ConsumerLockless() noexcept;

    ConsumerLockless(SingleData& cache, std::array<LocklessTasks, kRunProducerNum>& tasks);

    void start_thread_loop();
    void wait_until_join();
    // called by main thread to signal consumer thread need to exit
    void set_exit_task();

    std::tuple<size_t, size_t> get_stats() const;

private:
    void consumer_thread_loop();
    void clear_before_get_tasks(std::array<std::array<const std::string*, kLockLessArrayNum>, kRunProducerNum>& tasks) const;
    size_t get_requests(std::array<std::array<const std::string*, kLockLessArrayNum>, kRunProducerNum>& tasks);
    void procees_requests(const std::array<std::array<const std::string*, kLockLessArrayNum>, kRunProducerNum>& tasks);
};


}   // end of namespace cmp_mem_engine