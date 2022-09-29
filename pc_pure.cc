#include "pc_pure.h"

#include <iostream>

namespace cmp_mem_engine
{

ConsumerPure::ConsumerPure(SingleData& cache, Tasks& tasks)
    : Consumer(cache, tasks)
{}

void ConsumerPure::consumer_thread_loop_impl()
{
    using namespace std::chrono_literals;

    constexpr long int kCheckMs = 100;
    std::chrono::high_resolution_clock::time_point check = std::chrono::high_resolution_clock::now();
    size_t check_cnt = 0;

    bool busy_mode = true;
    while (true)
    {
        size_t consumed_cnt = process(nullptr);

        switch (consumed_cnt)
        {
        case kPidMaxMeaninngExit:
            return;         // exit consumer thread
        
        case 0:
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

            break;

        default:
            // some task is coming
            busy_mode = true;
            check_cnt = 0;
            bench_cnt_ += consumed_cnt;

            break;
        }
    }
}

void ConsumerPure::set_exit_task_more()
{
    // Do nothing at all
}

std::tuple<size_t, size_t, size_t> ConsumerPure::get_stats() const
{
    return {wait_try_cnt_, sleep_cnt_, bench_cnt_};
}

ProducerPure::ProducerPure(const size_t pid, Tasks& tasks, const std::vector<std::string>& samples)
    : Producer(pid, tasks, samples), pid_(pid)
{}

void ProducerPure::batch_keys(std::vector<std::string*>& keys)
{
    using namespace std::chrono_literals;

    while (!keys.empty())
    {
        // client/server mode
        // first the client (producer) send a request, i.e.,  process(pid_, keys)
        const size_t input_num = process(pid_, keys);
        if (input_num == 0)
        {
            ++sleep_cnt_;
            std::this_thread::sleep_for(1us);
            continue;
        }

        // We delete the input_num keys (all are pointers, so no deallocation of memory) from keys
        keys.erase(keys.begin(), keys.begin()+input_num);

        // then the client (the producer) waiting for the answer
        size_t this_most = 0;
        while (true)
        {
            auto outputs = prepare_outputs();
            process(pid_, outputs);

            if (outputs.size() == 0)
            {
                ++batch_fail_try_cnt_;
                ++this_most;
            }
            else
            {
                assert(outputs.size() == input_num);
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
               
                break;      // finish the some of this batch keys
            }
        }

        if (this_most > batch_fail_try_most_)
            batch_fail_try_most_ = this_most;
    }
}

int ProducerPure::miss_percent() const
{
    const size_t total = hit_cnt_ + miss_cnt_;

    return total == 0 ? 0 : static_cast<int>(miss_cnt_ * 100 / total);
}

size_t ProducerPure::sleep_count() const
{
    return sleep_cnt_;
}

std::tuple<size_t, size_t> ProducerPure::get_batch_fail_try_stats() const
{
    return {batch_fail_try_cnt_, batch_fail_try_most_};
}


}   // namespace of cmp_mem_engine