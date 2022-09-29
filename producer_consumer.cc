#include "producer_consumer.h"

#include <cassert>
#include <functional>
#include <iostream>
#include <atomic>
#include <cstring>

namespace cmp_mem_engine
{

const char* kNotFound = "Not Found Value";

Tasks::Tasks()
{
    for (size_t i = 0; i != kTaskLen; ++i)
    {
        tasks_[i].key = nullptr;
        tasks_[i].val = nullptr;
        tasks_[i].pid = 0;
    }

#ifdef USE_SPINLOCK_FOR_TASKS
    const int res = pthread_spin_init(&spinlock_, PTHREAD_PROCESS_PRIVATE);
    assert(res == 0);
#endif
}

Tasks::~Tasks()
{
#ifdef USE_SPINLOCK_FOR_TASKS
    const int res = pthread_spin_destroy(&spinlock_);
    assert(res == 0);
#endif
}

// caller guarantee to use lock
void Tasks::producer_dealwith_output(const size_t pid, std::vector<Output>& outputs)
{
    // caller must guarantee the outputs has accurate capacity (no need for memory allocation in lock)
    assert(outputs.empty() && outputs.capacity() == kTaskLen);

    for (size_t i = 0; i != kTaskLen; ++i)
    {
        if (pid != tasks_[i].pid)
            continue;

        if (tasks_[i].val != nullptr)
        {
            // found consumer thread finish this task of the same pid
            outputs.emplace_back(tasks_[i].key, tasks_[i].val);

            tasks_[i].pid = 0;
            tasks_[i].key = nullptr;
            tasks_[i].val = nullptr;
        }
    }
}

// calleer guarantee to use lock, return how many input keys input to tasks (from begin)
// return the number of task putting into task_, 0 meaning the task_ is full 
// NOTE: The caller needs guarantee input_keys is not empty
size_t Tasks::producer_dealwith_input(const size_t pid, const std::vector<std::string*>& input_keys)
{
    assert(pid != kPidZeroMeaningEmpty);

    // find the first index position in tasks_ which is empty
    size_t index = kTaskLen;
    for (size_t i = 0; i != kTaskLen; ++i)
    {
        if (tasks_[i].pid == kPidZeroMeaningEmpty)
        {
            index = i;
            break;
        }
    }

    if (index == kTaskLen)
        return 0;       // tasks_ is full

    // tasks_ is not empty from index position
    size_t cnt = 0;
    for (size_t i = 0; i != input_keys.size(); ++i)
    {
        assert(tasks_[index].pid == kPidZeroMeaningEmpty);
        assert(tasks_[index].key == nullptr);
        assert(tasks_[index].val == nullptr);

        tasks_[index].pid = pid;
        tasks_[index].key = input_keys[i];
        ++cnt;

        // find next available index
        for (index = index + 1; index != kTaskLen; ++index)
        {
            if (tasks_[index].pid  == kPidZeroMeaningEmpty)
                break;
        }

        if (index == kTaskLen)
            break;      // reach the end of tasks_, i.e., tasks_ is full
    }

    return cnt;
}

// only for outputs
void Tasks::producer_process(const size_t pid, std::vector<Output>& outputs)
{
    assert(pid != kPidZeroMeaningEmpty && pid != kPidMaxMeaninngExit);
    assert(outputs.empty());

#ifdef USE_SPINLOCK_FOR_TASKS
    int res = pthread_spin_lock(&spinlock_);
    assert(res == 0);
#else
    std::lock_guard<std::mutex> lk(mutex_);
#endif

    producer_dealwith_output(pid, outputs);

#ifdef USE_SPINLOCK_FOR_TASKS
    res = pthread_spin_unlock(&spinlock_);
    assert(res == 0);
#endif
}

// only for inputs
size_t Tasks::producer_process(const size_t pid, const std::vector<std::string*>& input_keys)
{
    assert(pid != kPidZeroMeaningEmpty && pid != kPidMaxMeaninngExit);
    assert(!input_keys.empty());

#ifdef USE_SPINLOCK_FOR_TASKS
    int res = pthread_spin_lock(&spinlock_);
    assert(res == 0);
#else
    std::lock_guard<std::mutex> lk(mutex_);
#endif

    const size_t num = producer_dealwith_input(pid, input_keys);

#ifdef USE_SPINLOCK_FOR_TASKS
    res = pthread_spin_unlock(&spinlock_);
    assert(res == 0);
#endif

    return num;
}

// both: inputs and outputs
size_t Tasks::producer_process(const size_t pid, 
                               const std::vector<std::string*>& input_keys, std::vector<Output>& outputs)
{
    assert(pid != kPidZeroMeaningEmpty && pid != kPidMaxMeaninngExit);
    assert(!input_keys.empty() && outputs.empty());

#ifdef USE_SPINLOCK_FOR_TASKS
    int res = pthread_spin_lock(&spinlock_);
    assert(res == 0);
#else
    std::lock_guard<std::mutex> lk(mutex_);
#endif

    // deal output first (so we can make some task empty for the following input step)
    producer_dealwith_output(pid, outputs);

    // then deal input
    const size_t num = producer_dealwith_input(pid, input_keys);

#ifdef USE_SPINLOCK_FOR_TASKS
    res = pthread_spin_unlock(&spinlock_);
    assert(res == 0);
#endif

    return num;
}

void Tasks::add_exit_task()
{
#ifdef USE_SPINLOCK_FOR_TASKS
    int res = pthread_spin_lock(&spinlock_);
    assert(res == 0);
#else
    std::lock_guard<std::mutex> lk(mutex_);
#endif

    assert(tasks_[0].pid == kPidZeroMeaningEmpty 
           && tasks_[0].key == nullptr
           && tasks_[0].val == nullptr);      // must be available, usually all task are finished and taken

    tasks_[0].pid = kPidMaxMeaninngExit;

#ifdef USE_SPINLOCK_FOR_TASKS
    res = pthread_spin_unlock(&spinlock_);
    assert(res == 0);
#endif
}

/* The caller guarantee pids are all false before call-in 
 * If one pid is processed, set pids[pid-1] to true,
 * so the caller (consumer) can signal the assocaated producers to process in async way
 * if pids is not nullptr (i.e., if pids is nullptr, the producer/consumer does not care) */
size_t Tasks::consumer_process(SingleData &cache, std::array<bool, kFixProducerNumber>* pids)
{
    std::array<const std::string*, kTaskLen> keys;
    std::array<size_t, kTaskLen> indexs;
    
    // First take some unfinished tasks
    size_t consumed_cnt = 0;
#ifdef USE_SPINLOCK_FOR_TASKS
    int res = pthread_spin_lock(&spinlock_);
    assert(res == 0);
#else
    std::unique_lock<std::mutex> lk(mutex_);
#endif
    for (size_t i = 0; i != kTaskLen; ++i)
    {
        if (tasks_[i].pid == kPidMaxMeaninngExit)
            return kPidMaxMeaninngExit;

        if (tasks_[i].pid == kPidZeroMeaningEmpty)
            continue;

        if (tasks_[i].val != nullptr)
            continue;       // not taken by producer, i.e., the task already done by consumer

        keys[consumed_cnt] = tasks_[i].key;
        indexs[consumed_cnt] = i;
        ++consumed_cnt;
    }
#ifdef USE_SPINLOCK_FOR_TASKS
    res = pthread_spin_unlock(&spinlock_);
    assert(res == 0);
#else
    lk.unlock();
#endif

    if (consumed_cnt == 0)
        return 0;       // no any one available task

    // Second, find the results in cache without lock
    std::array<const std::string*, kTaskLen> vals;
    for (size_t i = 0; i != consumed_cnt; ++i)
    {
        const std::string& key = *keys[i];
        const std::string* val = cache.find_val(key);

        if (val == nullptr)
        {
            // not found, but we can not put nullptr in vals, using an literal pointer instead
            vals[i] = reinterpret_cast<const std::string*>(kNotFound);
        }
        else
        {
            vals[i] = val;
        }
    }

    // Last, lock for write results to tasks_
#ifdef USE_SPINLOCK_FOR_TASKS
    res = pthread_spin_lock(&spinlock_);
    assert(res == 0);
#else
    lk.lock();
#endif
    for (size_t i = 0; i != consumed_cnt; ++i)
    {
        const size_t index = indexs[i];

        assert(tasks_[index].key == keys[i]);
        assert(tasks_[index].val == nullptr);
        assert(vals[i] != nullptr);

        tasks_[index].val = vals[i];
        const size_t pid = tasks_[index].pid;
        assert(pid != kPidZeroMeaningEmpty);
        if (pids != nullptr)
            (*pids)[pid-1] = true;
    }
#ifdef USE_SPINLOCK_FOR_TASKS
    res = pthread_spin_unlock(&spinlock_);
    assert(res == 0);
#else
    lk.unlock();
#endif

    return consumed_cnt;
}

Consumer::Consumer(SingleData& cache, Tasks& tasks) 
    : cache_(cache), tasks_(tasks)
{}

Consumer::~Consumer()
{
    try
    {
        wait_until_join();
    }
    catch (std::system_error& e)
    {
        std::cerr << "~Consumer thread join failed, reason = " << e.what() << '\n';
    }
}

void Consumer::consumer_thread_loop()
{
    consumer_thread_loop_impl();
}

size_t Consumer::process(std::array<bool, kFixProducerNumber>* pids)
{
    return tasks_.consumer_process(cache_, pids);
}

void Consumer::start_thread_loop()
{
    // std::thread t(consumer_thread_loop, std::ref(cache_), std::ref(tasks_), std::ref(task_flags_));
    std::thread t(&Consumer::consumer_thread_loop, this);
    thread_ = std::move(t);
}

void Consumer::wait_until_join()
{
    if (thread_.joinable())
        thread_.join();
}

void Consumer::set_exit_task()
{
    tasks_.add_exit_task();
    
    set_exit_task_more();
}


Producer::Producer(const size_t pid, Tasks& tasks, const std::vector<std::string>& samples)
    : re_(pid), tasks_(tasks)
{
    hot_keys_ = samples;

    random_keys_.reserve(samples.size());

    for (size_t i = 0; i != samples.size(); ++i)
    {
        std::string key = rand_str_scope(re_, kKeyMinLen, kKeyMaxLen);

        random_keys_.push_back(std::move(key));
    }
}

Producer::~Producer()
{
    try
    {
        wait_until_join();
    }
    catch(std::system_error& e)
    {
        std::cerr << "~Producer() failed when join, reason = " << e.what() << '\n';
    }
}

// allocating memory is OK for producer before call producer_process()
std::vector<std::string*> Producer::prepare_input_keys(const size_t num)
{
    assert(num > 0);

    std::vector<std::string*> keys;
    keys.reserve(num);

    for (size_t i = 0; i != num; ++i)
    {
        const int dice = re_.rand_int_scope(0, 99);
        if (dice < 90)
        {
            const size_t index = re_.rand_size_scope(0, hot_keys_.size());
            keys.emplace_back(&hot_keys_[index]);    
        }
        else
        {
            const size_t index = re_.rand_size_scope(0, random_keys_.size());
            keys.emplace_back(&random_keys_[index]);
        }
    }

    return keys;
}

// allocatimng memory is OK for producer before call producer_process() 
std::vector<Tasks::Output> Producer::prepare_outputs() const
{
    std::vector<Tasks::Output> outputs;
    outputs.reserve(kTaskLen);
    return outputs;
}

size_t Producer::process(const size_t pid, const std::vector<std::string*>& input_keys)
{
    return tasks_.producer_process(pid, input_keys);
}

void Producer::process(const size_t pid, std::vector<Tasks::Output>& outputs)
{
    tasks_.producer_process(pid, outputs);
}


void Producer::benchmark()
{
    using namespace std::chrono_literals;

    time_start_ = std::chrono::high_resolution_clock::now();

    size_t cnt = 0;

    while (cnt < kBenchmarkCount)
    {
        // We assume each step of a transaction need to read [kTransactionOneStepLeastKeys, kTransactionOneStepMostKeys] keys
        const size_t key_batch_num = re_.rand_size_scope(kTransactionOneStepLeastKeys, kTransactionOneStepMostKeys+1);
        auto keys = prepare_input_keys(key_batch_num);

        batch_keys(keys);

        cnt += key_batch_num;
    }

    time_end_ = std::chrono::high_resolution_clock::now();

    bench_cnt_ = cnt;
}

std::tuple<std::chrono::high_resolution_clock::time_point, std::chrono::high_resolution_clock::time_point>
Producer::get_time_points() const
{
    return {time_start_, time_end_};
}

size_t Producer::get_bench_count() const
{
    return bench_cnt_;
}

void Producer::start_thread()
{
    std::thread t(&Producer::benchmark, this);
    thread_ = std::move(t);
}

void Producer::wait_until_join()
{
    if (thread_.joinable())
        thread_.join();
}

}   // cmp_mem_engine