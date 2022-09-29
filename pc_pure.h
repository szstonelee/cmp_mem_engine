#pragma once

#include "producer_consumer.h"

namespace cmp_mem_engine
{

class ConsumerPure : public Consumer
{
private:
    size_t sleep_cnt_ = 0;
    size_t bench_cnt_ = 0;
    size_t wait_try_cnt_ = 0;

public:
    ConsumerPure() = delete;
    ConsumerPure(const ConsumerPure&) = delete;
    ConsumerPure(ConsumerPure&&) = delete;
    ConsumerPure& operator=(const ConsumerPure&) = delete;
    ConsumerPure& operator=(ConsumerPure&&) = delete;

    virtual ~ConsumerPure() = default;

    ConsumerPure(SingleData& cache, Tasks& tasks);

    std::tuple<size_t, size_t, size_t> get_stats() const;

private:
    void consumer_thread_loop_impl() override;
    void set_exit_task_more() override;

};

class ProducerPure : public Producer
{
private:
    const size_t pid_;

    size_t hit_cnt_ = 0;
    size_t miss_cnt_ = 0;
    size_t sleep_cnt_ = 0;
    size_t batch_fail_try_cnt_ = 0;
    size_t batch_fail_try_most_ = 0;

public:
    ProducerPure() = delete;
    ProducerPure(const ProducerPure&) = delete;
    ProducerPure(ProducerPure&&) = delete;
    ProducerPure& operator=(const ProducerPure&) = delete;
    ProducerPure& operator=(ProducerPure&&) = delete;

    virtual ~ProducerPure() = default;

    explicit ProducerPure(const size_t pid, Tasks& tasks, const std::vector<std::string>& samples);

    int miss_percent() const;
    size_t sleep_count() const;
    std::tuple<size_t, size_t> get_batch_fail_try_stats() const;

private:
    void batch_keys(std::vector<std::string*>& keys) override;    
};

}   // namespace of cmp_mem_engine