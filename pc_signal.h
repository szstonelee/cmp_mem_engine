#pragma once

#include "producer_consumer.h"

namespace cmp_mem_engine
{

struct AlignAtomicBool
{
    // alignas(hardware_destructive_interference_size) std::atomic<bool> atomic_bool;
    std::atomic<bool> atomic_bool;
};

struct TaskFlags
{
    std::array<AlignAtomicBool, cmp_mem_engine::kFixProducerNumber> flags;
};


class ConsumerSignal : public Consumer
{
private:
    TaskFlags& task_flags_;

    size_t wait_try_cnt_ = 0;
    size_t sleep_cnt_ = 0;
    size_t bench_cnt_ = 0;

public:
    ConsumerSignal() = delete;
    ConsumerSignal(const ConsumerSignal&) = delete;
    ConsumerSignal(ConsumerSignal&&) = delete;
    ConsumerSignal& operator=(const ConsumerSignal&) = delete;
    ConsumerSignal& operator=(ConsumerSignal&&) = delete;

    virtual ~ConsumerSignal() = default;

    ConsumerSignal(SingleData& cache, Tasks& tasks, TaskFlags& task_flags);

    std::tuple<size_t, size_t, size_t> get_stats() const;

private:
    void consumer_thread_loop_impl() override;
    void set_exit_task_more() override;

private:
    bool has_task_possible();
};

class ProducerSignal : public Producer
{
private:
    const size_t pid_;

    TaskFlags& task_flags_;

    size_t hit_cnt_ = 0;
    size_t miss_cnt_ = 0;
    size_t sleep_cnt_ = 0;
    size_t batch_fail_try_cnt_ = 0;
    size_t batch_fail_try_most_ = 0;

public:
    ProducerSignal() = delete;
    ProducerSignal(const ProducerSignal&) = delete;
    ProducerSignal(ProducerSignal&&) = delete;
    ProducerSignal& operator=(const ProducerSignal&) = delete;
    ProducerSignal& operator=(ProducerSignal&&) = delete;

    virtual ~ProducerSignal() = default;

    ProducerSignal(const size_t pid, Tasks& tasks, const std::vector<std::string>& samples, 
                  TaskFlags& task_flags);

    int miss_percent() const;
    size_t sleep_count() const;
    std::tuple<size_t, size_t> get_batch_fail_try_stats() const;

private:
    void batch_keys(std::vector<std::string*>& keys) override;    
};

}   // namespace of cmp_mem_engine