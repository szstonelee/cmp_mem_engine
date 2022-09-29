#include "pc_lockless.h"

#include <iostream>
#include <cstdlib>

namespace cmp_mem_engine
{

const char* kExitConsumerThreadTask = "This is the exit task for consumer thread";

ProducerLockless::ProducerLockless(const size_t pid, LocklessTasks& tasks, const std::vector<std::string>& samples)
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

ProducerLockless::~ProducerLockless()
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

void ProducerLockless::wait_until_join()
{
    if (thread_.joinable())
        thread_.join();
}

void ProducerLockless::start_thread()
{
    std::thread t(&ProducerLockless::benchmark, this);
    thread_ = std::move(t);
}

std::vector<const std::string*> ProducerLockless::prepare_input_keys(const size_t num)
{
    assert(num > 0);

    std::vector<const std::string*> keys;
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

size_t ProducerLockless::avail_slot_in_requests(const std::array<bool, kLockLessArrayNum>& is_processing, const size_t start_slot) const
{
    for (size_t i = start_slot; i != kLockLessArrayNum; ++i)
    {
        if (!is_processing[i])
        {
            assert(tasks_.request_keys[i].load(std::memory_order_relaxed) == nullptr);
            return i;
        }
    }

    return kLockLessArrayNum;   // not found
}

// find some free slots in tasks_ and add some task (from keys) to the tasks_
// after adding the tasks, remove them from keys
// remember these added keys in is_processing and processing_keys
// because consumer thread will clear tasks_
// return how many task have benn added for this turn
size_t ProducerLockless::fill_requests(std::vector<const std::string*>& keys, 
                                       std::array<bool, kLockLessArrayNum>& is_processing,
                                       std::array<const std::string*, kLockLessArrayNum>& processing_keys)
{
    size_t slot = avail_slot_in_requests(is_processing, 0);
    if (slot == kLockLessArrayNum)
        return 0;

    std::vector<size_t> indexs_in_keys;
    indexs_in_keys.reserve(kLockLessArrayNum);

    // for each key, find an available slot in tasks and add the task
    for (size_t i = 0; i != keys.size(); ++i)
    {
        const std::string* key = keys.at(i);
        assert(key != nullptr);
        tasks_.request_keys[slot].store(key, std::memory_order_release);    // for consumer thread
        is_processing[slot] = true;
        processing_keys[slot] = key;

        indexs_in_keys.push_back(i);

        slot = avail_slot_in_requests(is_processing, slot+1);
        if (slot == kLockLessArrayNum)
            break;
    }

    // remove already-added task from keys
    for (auto it = indexs_in_keys.crbegin(); it != indexs_in_keys.crend(); ++it)
    {
        size_t index_in_keys = *it;
        keys.erase(keys.begin()+index_in_keys);
    }

    return indexs_in_keys.size();
}

// Get results for those processing keys (check tasks_ which is served in consumer thread)
// After retrieve all results, remove those processed from is_processing and processing_keys,
// so fill_requests can work for next round of loop
// Return how many requests have benn servered (i.e., have results)
size_t ProducerLockless::get_results(std::array<bool, kLockLessArrayNum>& is_processing,
                                     std::array<const std::string*, kLockLessArrayNum>& processing_keys)
{
    size_t cnt = 0;

    for (size_t i = 0; i != kLockLessArrayNum; ++i)
    {
        if (!is_processing[i])
            continue;

        const std::string* result = tasks_.result_vals[i].load(std::memory_order_acquire);

        if (result == nullptr)
            continue;   // consumer thread has not servered the tasks

        // get real result
        processing_keys[i] = result;
        if (result == reinterpret_cast<const std::string*>(kNotFound))
        {
            ++miss_cnt_;
        }
        else
        {
            ++hit_cnt_;
        }

        // for next fill_requests()
        assert(tasks_.request_keys[i].load(std::memory_order_relaxed) == nullptr);
        is_processing[i] = false;
        processing_keys[i] = nullptr;
        tasks_.result_vals[i].store(nullptr, std::memory_order_relaxed);
        ++cnt;
    }

    return cnt;
}

void ProducerLockless::batch_keys(std::vector<const std::string*>& keys, const size_t debug_loop_no)
{
    // debug
    for (size_t i = 0; i != kLockLessArrayNum; ++i)
    {
        const std::string* check = tasks_.request_keys[i].load(std::memory_order_relaxed);
        if (check != nullptr)
        {
            std::cerr << "batch_keys debug failed, i = " << i 
                      << ", debug_loop_no = " << debug_loop_no
                      << '\n';
            exit(1);
        }
    }

    std::array<bool, kLockLessArrayNum> is_processing;
    is_processing.fill(false);
    std::array<const std::string*, kLockLessArrayNum> processing_keys; 
    processing_keys.fill(nullptr);

    size_t request_most = 0;
    size_t result_most = 0;

    size_t answer_cnt = 0;
    const size_t total_anser = keys.size();

    while (answer_cnt != total_anser)
    {
        // fill the request looply
        if (!keys.empty())
        {
            const size_t added_in_this_turn = fill_requests(keys, is_processing, processing_keys);

            if (added_in_this_turn == 0)
            {
                ++batch_request_wait_cnt_;
                ++request_most;
            }
            else
            {
                if (request_most > batch_request_most_)
                    batch_request_most_ = request_most;

                request_most = 0;
            }
        }

        // check the results looply
        const size_t processed_in_this_turn = get_results(is_processing, processing_keys);
        if (processed_in_this_turn == 0)
        {
            ++batch_ressult_wait_cnt_;
            ++result_most;
        }
        else
        {
            answer_cnt += processed_in_this_turn;

            if (result_most > batch_result_most_)
                batch_result_most_ = result_most;

            result_most = 0;
        }
    }
}

void ProducerLockless::benchmark()
{
    using namespace std::chrono_literals;

    time_start_ = std::chrono::high_resolution_clock::now();

    size_t cnt = 0;

    size_t debug_loop_no = 0;

    while (cnt < kBenchmarkCount)
    {
        // We assume each step of a transaction need to read [kTransactionOneStepLeastKeys, kTransactionOneStepMostKeys] keys
        const size_t key_batch_num = re_.rand_size_scope(kTransactionOneStepLeastKeys, kTransactionOneStepMostKeys+1);
        auto keys = prepare_input_keys(key_batch_num);

        batch_keys(keys, debug_loop_no);
        ++debug_loop_no;

        cnt += key_batch_num;
    }

    time_end_ = std::chrono::high_resolution_clock::now();

    bench_cnt_ = cnt;
}

int ProducerLockless::miss_percent() const
{
    const size_t total = hit_cnt_ + miss_cnt_;

    return total == 0 ? 0 : static_cast<int>(miss_cnt_ * 100 / total);
}

size_t ProducerLockless::get_bench_count() const
{
    return bench_cnt_;
}

std::tuple<size_t, size_t, size_t, size_t> ProducerLockless::get_batch_stats() const
{
    return {batch_request_wait_cnt_, batch_request_most_, batch_ressult_wait_cnt_, batch_result_most_};
}

std::tuple<std::chrono::high_resolution_clock::time_point, std::chrono::high_resolution_clock::time_point>
ProducerLockless::get_time_points() const
{
    return {time_start_, time_end_};
}

void ProducerLockless::debug_address_of_tasks() const
{
    std::cout << "address of tasks = " << &tasks_ << '\n';
}

ConsumerLockless::ConsumerLockless(SingleData& cache, std::array<LocklessTasks, kRunProducerNum>& tasks)
    : cache_(cache), producer_tasks_(tasks)
{}

ConsumerLockless::~ConsumerLockless()
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

void ConsumerLockless::start_thread_loop()
{
    std::thread t(&ConsumerLockless::consumer_thread_loop, this);
    thread_ = std::move(t);
}

void ConsumerLockless::wait_until_join()
{
    if (thread_.joinable())
        thread_.join();
}

// called by main thread. 
// The main thread needs to guarantee that
// all tasks have been finished (i.e., all producer threads exits)
void ConsumerLockless::set_exit_task()
{
    producer_tasks_[0].request_keys[0].store(
        reinterpret_cast<const std::string*>(kExitConsumerThreadTask), std::memory_order_relaxed);
}

void ConsumerLockless::clear_before_get_tasks(std::array<std::array<const std::string*, kLockLessArrayNum>, kRunProducerNum>& tasks) const
{
    for (size_t i = 0; i != kRunProducerNum; ++i)
    {
        tasks[i].fill(nullptr);
    }
}

size_t ConsumerLockless::get_requests(std::array<std::array<const std::string*, kLockLessArrayNum>, kRunProducerNum>& tasks)
{
    clear_before_get_tasks(tasks);
    size_t cnt = 0;

    for (size_t i = 0; i != kRunProducerNum; ++i)
    {
        for (size_t j = 0; j != kLockLessArrayNum; ++j)
        {
            const std::string* task = producer_tasks_[i].request_keys[j].load(std::memory_order_acquire);
            if (task != nullptr)
            {
                tasks[i][j] = task;
                producer_tasks_[i].request_keys[j].store(nullptr, std::memory_order_relaxed);
                ++cnt;
            }
        }
    }

    return cnt;
}

void ConsumerLockless::procees_requests(const std::array<std::array<const std::string*, kLockLessArrayNum>, kRunProducerNum>& tasks)
{
    for (size_t i = 0; i != kRunProducerNum; ++i)
    {
        for (size_t j = 0; j != kLockLessArrayNum; ++j)
        {
            const std::string* task = tasks[i][j];
            if (task != nullptr)
            {
                assert(producer_tasks_[i].result_vals[j].load(std::memory_order_relaxed) == nullptr);
                const std::string* result = cache_.find_val(*task);
                if (result == nullptr)
                {
                    producer_tasks_[i].result_vals[j].store(
                            reinterpret_cast<const std::string*>(kNotFound), std::memory_order_release);
                }
                else
                {
                    producer_tasks_[i].result_vals[j].store(result, std::memory_order_release);
                }
            }
        }
    }
}

void ConsumerLockless::consumer_thread_loop()
{
    using namespace std::chrono_literals;

    std::array<std::array<const std::string*, kLockLessArrayNum>, kRunProducerNum> tasks;

    while (true)
    {
        // check exit task first
        if (producer_tasks_[0].request_keys[0].load(std::memory_order_relaxed) == 
            reinterpret_cast<const std::string*>(kExitConsumerThreadTask))
            break;
        
        const size_t request_cnt = get_requests(tasks);

        if (request_cnt != 0)
        {
            procees_requests(tasks);
            batch_cnt_ += request_cnt;
        }

        if (batch_cnt_ == 0 || batch_cnt_ >= kRunProducerNum * kBenchmarkCount)
        {
            std::this_thread::sleep_for(100us);
        }
        else
        {
            if (request_cnt == 0)
                ++wait_cnt_;
        }
    }
}

std::tuple<size_t, size_t> ConsumerLockless::get_stats() const
{
    return {batch_cnt_, wait_cnt_};
}

}   // end of namespace cmp_mem_engine