#include <iostream>
#include <chrono>
#include <vector>
#include <memory>
#include <atomic>

#include "const_and_share_struct.h"
#include "single_thread.h"
#include "multi_threads.h"
#include "producer_consumer.h"
#include "pc_signal.h"
#include "pc_pure.h"
#include "pc_lockless.h"

std::string size_to_str(std::size_t num)
{
    std::string res;

    if (num == 0)
    {
        res = std::to_string(0);
        return res;
    }

    const int g_part = num / (1<<30);
    if (g_part != 0)
    {
        res += std::to_string(g_part) + " g ";
        num -= (size_t)g_part * (1<<30);
    }

    const int m_part = num / (1<<20);
    if (m_part != 0)
    {
        res += std::to_string(m_part) + " m ";
        num -= (size_t)m_part * (1<<20);
    }

    if (g_part != 0)
        return res;

    const int k_part = num / (1<<10);
    if (k_part)
    {
        res += std::to_string(k_part) + " k ";
        num -= (size_t)k_part * (1<<10);
    }

    if (m_part != 0)
        return res;

    if (num != 0)
        res += std::to_string(num);

    return res;
}

void benchmark_single()
{
    std::cout << "benchmark single test starting ...\n";
    std::chrono::high_resolution_clock::time_point begin = std::chrono::high_resolution_clock::now(); 
    cmp_mem_engine::Single s(cmp_mem_engine::kKeySpace, cmp_mem_engine::kHotSpace, cmp_mem_engine::kRandSpace);
    std::chrono::high_resolution_clock::time_point end = std::chrono::high_resolution_clock::now();
    std::chrono::seconds duration_init = std::chrono::duration_cast<std::chrono::seconds>(end - begin);
    std::cout << "Single thread init duration(s) = " << duration_init.count() << '\n';

    begin = std::chrono::high_resolution_clock::now();
    s.benchmark();
    end = std::chrono::high_resolution_clock::now();
    std::chrono::milliseconds duration_lookup = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin);
    const size_t qps = cmp_mem_engine::kBenchmarkCount * 1000 / duration_lookup.count();
    std::cout << "lookup time(s) = " << duration_lookup.count() / 1000
              << ", qps = " <<  size_to_str(qps) 
              << ", miss percentage = " << s.miss_percent() << "%\n";
}

void benchmark_multi()
{
    std::cout << "benchmark multi test starting ...\n";

    std::chrono::high_resolution_clock::time_point begin = std::chrono::high_resolution_clock::now(); 
    std::vector<std::string> samples;
    std::shared_ptr<cmp_mem_engine::ShareData> data 
        = std::make_shared<cmp_mem_engine::ShareData>(cmp_mem_engine::kKeySpace, cmp_mem_engine::kSampleSpace, samples);
    std::chrono::high_resolution_clock::time_point end = std::chrono::high_resolution_clock::now();
    std::chrono::seconds duration_init = std::chrono::duration_cast<std::chrono::seconds>(end - begin);
    std::cout << "data hash table load factor = " << data->hash_table_load_factor() 
              << ", max load factor = " << data->max_hash_table_load_factor() << '\n';
    std::cout << "Multi threads init duration(s) = " << duration_init.count() << '\n';

    constexpr size_t kThreadNum = cmp_mem_engine::kRunProducerNum;
    std::vector<std::unique_ptr<cmp_mem_engine::Multi>> ms;
    ms.reserve(kThreadNum);
    for (size_t i = 0; i != kThreadNum; ++i)
    {
        ms.push_back(std::make_unique<cmp_mem_engine::Multi>(data, samples));
    }

    begin = std::chrono::high_resolution_clock::now();
    for (size_t i = 0; i != kThreadNum; ++i)
    {
        ms[i]->start_bench_in_thread(cmp_mem_engine::kBenchmarkCount);
    }
    for (size_t i = 0; i != kThreadNum; ++i)
    {
        ms[i]->wait_until_thread_finish();
    }
    end = std::chrono::high_resolution_clock::now();

    auto [min_time, max_time] = ms[0]->get_time_points(); 
    for (size_t i = 0; i != kThreadNum; ++i)
    {
        if (i != 0)
        {
            auto [cur_start, cur_end] = ms[i]->get_time_points();
            
            if (cur_start < min_time)
                min_time = cur_start;

            if (cur_end > max_time)
                max_time = cur_end;
        }

        const size_t qps = cmp_mem_engine::kBenchmarkCount * 1000 / ms[i]->duration().count();
        const int miss = ms[i]->miss_percent();
        std::cout << "Thread id = " << i << ", qps = " << size_to_str(qps) << " , miss = " << miss << "%\n";
    }

    const size_t query_total = cmp_mem_engine::kBenchmarkCount * kThreadNum;    

    const std::chrono::milliseconds duration_threads = std::chrono::duration_cast<std::chrono::milliseconds>(max_time - min_time);
    const size_t qps_threads = query_total * 1000 / duration_threads.count();
    std::cout << "Total " << kThreadNum << " threads, threads qps(total) = " << size_to_str(qps_threads) << "\n";

    const std::chrono::milliseconds duration_elapse = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin);
    const size_t qps_elapse = query_total * 1000 / duration_elapse.count();
    std::cout << "Total " << kThreadNum << " threads, elapse qps(total) = " << size_to_str(qps_elapse) <<  "\n";
}


void benchmark_producer_consumer_signal()
{
    using namespace std::chrono_literals;

    std::cout << "benchmark producer&consumer by signal, init starting ...\n";
    std::vector<std::string> samples;
    cmp_mem_engine::SingleData cache(cmp_mem_engine::kKeySpace, cmp_mem_engine::kSampleSpace, samples);
    cmp_mem_engine::Tasks tasks;
    std::cout << "producer&consumer init finish\n";

    // std::array<std::atomic<bool>, cmp_mem_engine::kFixProducerNumber> task_flags;
    cmp_mem_engine::TaskFlags task_flags;
    for (size_t i = 0; i != cmp_mem_engine::kFixProducerNumber; ++i)
    {
        // task_flags[i].store(false, std::memory_order_relaxed);
        task_flags.flags[i].atomic_bool.store(false, std::memory_order_relaxed);
    }

    // First, start only one consumer thread
    cmp_mem_engine::ConsumerSignal consumer(cache, tasks, task_flags);
    consumer.start_thread_loop();

    // then start kRunProducerNum producer threads
    constexpr size_t kProducerThreadNum = cmp_mem_engine::kRunProducerNum;
    std::vector<std::unique_ptr<cmp_mem_engine::ProducerSignal>> ps;
    ps.reserve(kProducerThreadNum);
    for (size_t i = 0; i != kProducerThreadNum; ++i)
    {
        auto one = std::make_unique<cmp_mem_engine::ProducerSignal>(i+1, tasks, samples, task_flags);
        ps.push_back(std::move(one));
    }

    for (size_t i = 0; i != kProducerThreadNum; ++i)
    {
        ps[i]->start_thread();
    }

    for (size_t i = 0; i != kProducerThreadNum; ++i)
    {
        ps[i]->wait_until_join();
    }

    // std::cout << "main thread sleep 20s after producer exit.\n";
    // std::this_thread::sleep_for(1s);

    // consumer thead exit after the producer threads have exited
    consumer.set_exit_task();
    consumer.wait_until_join();

    // output the results
    for (size_t i = 0; i != kProducerThreadNum; ++i)
    {
        auto& p = ps[i];
        const size_t bench_cnt = p->get_bench_count();
        auto [p_start, p_end] = p->get_time_points();
        const std::chrono::milliseconds duration_p = std::chrono::duration_cast<std::chrono::milliseconds>(p_end - p_start);
        const size_t p_qps = bench_cnt * 1000 / duration_p.count();
        auto [batch_fail_try_cnt, batch_fail_try_most] = p->get_batch_fail_try_stats();
        std::cout << "producer id = " << i + 1
                  << ", bench count = " << size_to_str(bench_cnt)
                  << ", qps = " << size_to_str(p_qps)
                  << ", miss percent = " << p->miss_percent() << "%"
                  << ", sleep count = " << p->sleep_count() 
                  << ", batch_fail_try_cnt = " << size_to_str(batch_fail_try_cnt)
                  << ", batch_fail_try_most = " << size_to_str(batch_fail_try_most)
                  << '\n';
    }

    auto [retry_cnt, sleep_cnt, bench_cnt] = consumer.get_stats();
    std::cout << "consumer wait and retry count = " << size_to_str(retry_cnt) 
              << ", sleep count = " << size_to_str(sleep_cnt) 
              << ", bench count = " << size_to_str(bench_cnt)
              << '\n';
}

void benchmark_producer_consumer_pure()
{
    std::cout << "benchmark producer&consumer by pure, init starting ...\n";
    std::vector<std::string> samples;
    cmp_mem_engine::SingleData cache(cmp_mem_engine::kKeySpace, cmp_mem_engine::kSampleSpace, samples);
    cmp_mem_engine::Tasks tasks;
    std::cout << "producer&consumer init finish\n";

    // First, start only one consumer thread
    cmp_mem_engine::ConsumerPure consumer(cache, tasks);
    consumer.start_thread_loop();

    // then start kRunProducerNum producer threads
    constexpr size_t kProducerThreadNum = cmp_mem_engine::kRunProducerNum;
    std::vector<std::unique_ptr<cmp_mem_engine::ProducerPure>> ps;
    ps.reserve(kProducerThreadNum);
    for (size_t i = 0; i != kProducerThreadNum; ++i)
    {
        auto one = std::make_unique<cmp_mem_engine::ProducerPure>(i+1, tasks, samples);
        ps.push_back(std::move(one));
    }

    for (size_t i = 0; i != kProducerThreadNum; ++i)
    {
        ps[i]->start_thread();
    }

    for (size_t i = 0; i != kProducerThreadNum; ++i)
    {
        ps[i]->wait_until_join();
    }

    // consumer thead exit after the producer threads have exited
    consumer.set_exit_task();
    consumer.wait_until_join();

    // output the results
    for (size_t i = 0; i != kProducerThreadNum; ++i)
    {
        auto& p = ps[i];
        const size_t bench_cnt = p->get_bench_count();
        auto [p_start, p_end] = p->get_time_points();
        const std::chrono::milliseconds duration_p = std::chrono::duration_cast<std::chrono::milliseconds>(p_end - p_start);
        const size_t p_qps = bench_cnt * 1000 / duration_p.count();
        auto [batch_fail_try_cnt, batch_fail_try_most] = p->get_batch_fail_try_stats();
        std::cout << "producer id = " << i + 1
                  << ", bench count = " << size_to_str(bench_cnt)
                  << ", qps = " << size_to_str(p_qps)
                  << ", miss percent = " << p->miss_percent() << "%"
                  << ", sleep count = " << p->sleep_count() 
                  << ", batch_fail_try_cnt = " << size_to_str(batch_fail_try_cnt)
                  << ", batch_fail_try_most = " << size_to_str(batch_fail_try_most)
                  << '\n';
    }

    auto [retry_cnt, sleep_cnt, bench_cnt] = consumer.get_stats();
    std::cout << "consumer wait and retry count = " << size_to_str(retry_cnt) 
              << ", sleep count = " << size_to_str(sleep_cnt) 
              << ", bench count = " << size_to_str(bench_cnt)
              << '\n';
}

void benchmark_producer_consumer_lockless()
{
    std::cout << "benchmark producer&consumer by lockless, init starting ...\n";
    std::vector<std::string> samples;
    cmp_mem_engine::SingleData cache(cmp_mem_engine::kKeySpace, cmp_mem_engine::kSampleSpace, samples);
    std::array<cmp_mem_engine::LocklessTasks, cmp_mem_engine::kRunProducerNum> producers_tasks;

    // First, start only one consumer thread
    cmp_mem_engine::ConsumerLockless consumer(cache, producers_tasks);
    consumer.start_thread_loop();

    // then start kRunProducerNum producer threads
    std::vector<std::unique_ptr<cmp_mem_engine::ProducerLockless>> ps;
    ps.reserve(cmp_mem_engine::kRunProducerNum);
    for (size_t i = 0; i != cmp_mem_engine::kRunProducerNum; ++i)
    {
        auto one = std::make_unique<cmp_mem_engine::ProducerLockless>(i+1, producers_tasks[i], samples);
        // one->debug_address_of_tasks();
        ps.push_back(std::move(one));
    }
    // exit(1);

    for (size_t i = 0; i != cmp_mem_engine::kRunProducerNum; ++i)
    {
        ps[i]->start_thread();
    }

    for (size_t i = 0; i != cmp_mem_engine::kRunProducerNum; ++i)
    {
        ps[i]->wait_until_join();
    }

    // consumer thead exit after the producer threads have exited
    consumer.set_exit_task();
    consumer.wait_until_join();

    // output the results
    for (size_t i = 0; i != cmp_mem_engine::kRunProducerNum; ++i)
    {
        auto& p = ps[i];
        const size_t bench_cnt = p->get_bench_count();
        auto [p_start, p_end] = p->get_time_points();
        const std::chrono::milliseconds duration_p = std::chrono::duration_cast<std::chrono::milliseconds>(p_end - p_start);
        const size_t p_qps = bench_cnt * 1000 / duration_p.count();
        auto [request_wait_cnt, request_wait_most, result_wait_cnt, ressult_wait_most] = p->get_batch_stats();
        std::cout << "producer id = " << i + 1
                  << ", bench count = " << size_to_str(bench_cnt)
                  << ", qps = " << size_to_str(p_qps)
                  << ", miss percent = " << p->miss_percent() << "%"
                  << ", request_wait_cnt = " << size_to_str(request_wait_cnt)
                  << ", request_wait_most = " << size_to_str(request_wait_most)
                  << ", result_wait_cnt = " << size_to_str(result_wait_cnt)
                  << ", ressult_wait_most = " << size_to_str(ressult_wait_most)
                  << '\n';
    }

    auto [batch_cnt, wait_cnt] = consumer.get_stats();
    std::cout << "consumer waiit count = " << size_to_str(wait_cnt) 
              << ", bench count = " << size_to_str(batch_cnt)
              << '\n';
}

int main()
{
    benchmark_producer_consumer_lockless();

    // benchmark_producer_consumer_pure();

    // benchmark_producer_consumer_signal();

    // benchmark_multi();

    // benchmark_single();

    return 0;
}