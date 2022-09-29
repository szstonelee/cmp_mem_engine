#pragma once

#include <mutex>
#include <thread>

#include <pthread.h>

#include "const_and_share_struct.h"


namespace cmp_mem_engine
{

class Tasks
{
public:
    struct Output
    {
        explicit Output(const std::string* k, const std::string* v) : key(k), val(v)
        {}

        const std::string* key;
        const std::string* val;
    };

private:
    struct TaskElement
    {
        const std::string* key;
        const std::string* val;
        size_t pid;
    };

private:
#ifdef USE_SPINLOCK_FOR_TASKS
    pthread_spinlock_t spinlock_;
#else
    mutable std::mutex mutex_;
#endif
    // char padding_[hardware_destructive_interference_size];
    std::array<TaskElement, kTaskLen> tasks_;

public:
    Tasks();
    ~Tasks();
    // Note: outputs should outputs.reserver(kTaskLen) and empty() before call-in
    //       which will avoid allocation for Task (because it use lock)
    // 
    // It is producer responsibility to guarantee to avoid no duplictated input key 
    // if key has already been processed (i.e., val != nullptr or no repeating of taken and input) 
    // NOTE: If happened, it is also OK but useless
    size_t producer_process(const size_t pid, 
                           const std::vector<std::string*>& input_keys, std::vector<Output>& outputs);
    void producer_process(const size_t pid, std::vector<Output>& outputs);
    size_t producer_process(const size_t pid, const std::vector<std::string*>& input_keys);

    // consume something, return the result
    // if return std::numeric_limits<size_t>::max(), it means consumer thread should exit
    // else the number of consuming task (may be zero)
    size_t consumer_process(SingleData& cache, std::array<bool, kFixProducerNumber>* pids);

    // NOTE: caller (main thread) should guarantee all produecr has exited and the consumer thread is alive only
    //       and all related work has been done corretly
    //       i.e., this should be the last task
    void add_exit_task();

private:
    void producer_dealwith_output(const size_t pid, std::vector<Output>& outputs);
    size_t producer_dealwith_input(const size_t pid, const std::vector<std::string*>& input_keys);
};

class Consumer
{
private:
    std::thread thread_;
    SingleData& cache_;
    Tasks& tasks_;
    // std::array<std::atomic<bool>, kFixProducerNumber>& task_flags_;


public:
    Consumer() = delete;
    Consumer(const Consumer&) = delete;
    Consumer(Consumer&&) = delete;
    Consumer& operator=(const Consumer&) = delete;
    Consumer& operator=(Consumer&&) = delete;

    virtual ~Consumer() noexcept;

    Consumer(SingleData& cache, Tasks& tasks);

    void start_thread_loop();
    void wait_until_join();
    // called by main thread to signal consumer thread need to exit
    void set_exit_task();

protected:
    /* The caller guarantee pids are all false before call-in 
     * If pids is nullptr, the consumer does not care aoub the pids
     * otherwise, 
     * when one pid is processed, set pids[pid-1] to true,
     * so the caller (consumer) can signal the assocaated producers to process in async way */    
    size_t process(std::array<bool, kFixProducerNumber>* pids);

// Interface of virtual function
private:
    virtual void consumer_thread_loop_impl() = 0;
    virtual void set_exit_task_more() = 0;

private:
    void consumer_thread_loop();
};

class Producer
{
private:
    RandomEngine re_;
    std::thread thread_;

    Tasks& tasks_;
    std::vector<std::string> hot_keys_;
    std::vector<std::string> random_keys_;

    std::chrono::high_resolution_clock::time_point time_start_;
    std::chrono::high_resolution_clock::time_point time_end_;

    size_t bench_cnt_ = 0;

    // std::array<std::atomic<bool>, cmp_mem_engine::kFixProducerNumber>& task_flags_;

public:
    Producer() = delete;
    Producer(const Producer&) = delete;
    Producer(Producer&&) = delete;
    Producer& operator=(const Producer&) = delete;
    Producer& operator=(Producer&&) = delete;

    virtual ~Producer() noexcept;

    Producer(const size_t pid, Tasks& tasks, const std::vector<std::string>& samples);
    void start_thread();
    void wait_until_join();

    std::tuple<std::chrono::high_resolution_clock::time_point, std::chrono::high_resolution_clock::time_point>
    get_time_points() const;
    size_t get_bench_count() const;

protected:
    void benchmark();
    size_t process(const size_t pid, const std::vector<std::string*>& input_keys);
    void process(const size_t pid, std::vector<Tasks::Output>& outputs);
    
    std::vector<std::string*> prepare_input_keys(const size_t num);
    std::vector<Tasks::Output> prepare_outputs() const;

private:
    virtual void batch_keys(std::vector<std::string*>& keys) = 0;
};


}   // cmp_mem_engine