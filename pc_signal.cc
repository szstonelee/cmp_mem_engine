#include "pc_signal.h"

#include <iostream>
#include <cassert>

namespace cmp_mem_engine
{

ConsumerSignal::ConsumerSignal(SingleData& cache, Tasks& tasks, TaskFlags& task_flags)
    : Consumer(cache, tasks), task_flags_(task_flags)
{}

void ConsumerSignal::set_exit_task_more()
{
    task_flags_.flags[0].atomic_bool.store(true, std::memory_order_relaxed);     // let consumer know
}

bool ConsumerSignal::has_task_possible()
{
    for (size_t i = 0; i != kFixProducerNumber; ++i)
    {
        const bool has_task_for_one_producer = task_flags_.flags[i].atomic_bool.load(std::memory_order_relaxed);
        if (has_task_for_one_producer)
            return true;
        
        /*
        bool exptected = true;
        const bool res = 
            task_flags_.flags[i].atomic_bool.compare_exchange_weak(exptected, true, 
                                                                  std::memory_order_relaxed, std::memory_order_relaxed);
        if (res || exptected)
            return true;
        */
    }

    return false;
}

void ConsumerSignal::consumer_thread_loop_impl()
{
    using namespace std::chrono_literals;

    // If in busy mode, consumer will loop for 100ms to check whether there is a task.
    // If no task at all, consumer will switch to **no busy mode**, which will sleep for 1ms then check again.
    // If a task is coming, we switch to busy mode.

    constexpr long int kCheckMs = 100;
    std::chrono::high_resolution_clock::time_point check = std::chrono::high_resolution_clock::now();
    size_t check_cnt = 0;

    std::array<bool, kFixProducerNumber> pids;

    bool busy_mode = true;
    while (true)
    {
        size_t consumed_cnt = 0;
        if (has_task_possible())
        {
            pids.fill(false);
            // NOTE: maybe no task for consumeer in tasks even has_task_possible() return true
            consumed_cnt = process(&pids);        

            if (consumed_cnt == kPidMaxMeaninngExit)
                return;     // terminate consume thread
        }

        if (consumed_cnt == 0)       
        {
            // no task
            if (!busy_mode)
            {
                if (bench_cnt_ > 0 && bench_cnt_ < kRunProducerNum * kBenchmarkCount)
                {
                    ++sleep_cnt_;
                    std::cout << "debug, bench_cnt_  = " << bench_cnt_ << '\n';
                }

                std::this_thread::sleep_for(1ms);
            }
            else
            {
                if (check_cnt == 0)
                    check = std::chrono::high_resolution_clock::now();

                ++check_cnt;

                if (check_cnt % (1<<10) == 0)
                {
                    // reduce the call to chrono to make efficiency by mod(%)
                    std::chrono::high_resolution_clock::time_point cur = std::chrono::high_resolution_clock::now();
                    std::chrono::milliseconds duration = std::chrono::duration_cast<std::chrono::milliseconds>(cur - check);                
                    if (duration.count() >= kCheckMs)
                        busy_mode = false;
                }
            }

            if (bench_cnt_ > 0 && bench_cnt_ < kRunProducerNum * kBenchmarkCount)
                ++wait_try_cnt_;
        }
        else
        {
            // some task is coming
            busy_mode = true;
            check_cnt = 0;
            bench_cnt_ += consumed_cnt;

            // deal with pids to let the producer know the tasks are finished by the consumer
            for (size_t i = 0; i != kFixProducerNumber; ++i)
            {
                if (pids[i])
                {
                    // this producer need to be notified
                    // assert(task_flags[i].load(std::memory_order_relaxed));
                    assert(task_flags_.flags[i].atomic_bool.load(std::memory_order_relaxed));
                    // task_flags[i].store(false, std::memory_order_relaxed);
                    task_flags_.flags[i].atomic_bool.store(false, std::memory_order_relaxed);
                    // task_flags[i].store(false, std::memory_order_seq_cst);
                }
            }
        }
    }
}

std::tuple<size_t, size_t, size_t> ConsumerSignal::get_stats() const
{
    return {wait_try_cnt_, sleep_cnt_, bench_cnt_};
}

ProducerSignal::ProducerSignal(const size_t pid, Tasks& tasks, const std::vector<std::string>& samples, 
                               TaskFlags& task_flags)
    : Producer(pid, tasks, samples), pid_(pid), task_flags_(task_flags)
{}

void ProducerSignal::batch_keys(std::vector<std::string*>& keys) 
{
    using namespace std::chrono_literals;

    while (!keys.empty())
    {
        // client/server mode
        // first the client (producer) send a request, i.e.,  process(pid_, keys)

        // before put key in tasks, we need to set the flag to true to signal consumer thread
        // the consumer thread maybe get no task associated with the produceer thread
        // but it does not matter (because consumer is looping for the check)
        // assert(!task_flags_[pid_-1].load(std::memory_order_relaxed));   // init value must be false indicating no taks needs to be consumed
        assert(!task_flags_.flags[pid_-1].atomic_bool.load(std::memory_order_relaxed));     // init value must be false indicating no taks needs to be consumed
        // task_flags_[pid_-1].store(true, std::memory_order_relaxed);     // signal consumer thread
        task_flags_.flags[pid_-1].atomic_bool.store(true, std::memory_order_relaxed);       // signal consumer thread

        const size_t input_num = process(pid_, keys);

        if (input_num == 0)
        {
            // the tasks are full (maybe by other producer or myself's previous part of batch keys)
            // task_flags_[pid_-1].store(false, std::memory_order_relaxed);
            task_flags_.flags[pid_-1].atomic_bool.store(false, std::memory_order_relaxed);
            ++sleep_cnt_;
            std::this_thread::sleep_for(1us);
            continue;
        }

        // We delete the input_num keys (all are pointers, so no deallocation of memory) from keys
        keys.erase(keys.begin(), keys.begin()+input_num);

        // then the client (the producer) waiting for the answer

        auto outputs = prepare_outputs();
        size_t this_most = 0;

        do 
        {
            // because we set task_flags_[pid_-1] of true before producer_process()
            // so we can check it now by looping for ever
            // waiting for consumer signal backs
            const bool signal_by_consumer = !task_flags_.flags[pid_-1].atomic_bool.load(std::memory_order_relaxed);
            
            /*
            bool expected = false;
            const bool res = task_flags_.flags[pid_-1].atomic_bool.compare_exchange_weak(expected, false,
                                                                                         std::memory_order_relaxed, std::memory_order_relaxed);
            bool signal_by_consumer = false;
            if (res || !expected)
                signal_by_consumer = true;     
            */       

            if (signal_by_consumer)
            {
                process(pid_, outputs);
                assert(outputs.size() == input_num);
                break;
            }
            else
            {
                ++batch_fail_try_cnt_;
                ++this_most;
            }
        } while (true);

        if (this_most > batch_fail_try_most_)
            batch_fail_try_most_ = this_most;

        for (size_t i = 0; i != input_num; ++i)
        {
            if (reinterpret_cast<const char*>(outputs[i].val) == kNotFound)
            {
                ++miss_cnt_;
            }
            else
            {
                ++hit_cnt_;
            }
        }
    }
}

int ProducerSignal::miss_percent() const
{
    const size_t total = hit_cnt_ + miss_cnt_;

    return total == 0 ? 0 : static_cast<int>(miss_cnt_ * 100 / total);
}

size_t ProducerSignal::sleep_count() const
{
    return sleep_cnt_;
}

std::tuple<size_t, size_t> ProducerSignal::get_batch_fail_try_stats() const
{
    return {batch_fail_try_cnt_, batch_fail_try_most_};
}

}   // namespace of cmp_mem_engine