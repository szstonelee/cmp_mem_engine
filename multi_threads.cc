#include "multi_threads.h"

#include <cassert>
#include <iostream>

#include "random_str.h"

namespace cmp_mem_engine
{

ShareData::ShareData(const size_t init_key_num, const size_t sample_key_num, std::vector<std::string>& samples)
{
    assert(samples.empty());

    RandomEngine re(0);
    // key_vals_.max_load_factor(0.5);
    key_vals_.reserve(init_key_num);
    samples.reserve(sample_key_num);

    size_t sample_cnt = 0, protected_cnt = 0;

    for (size_t i = 0; i != init_key_num; ++i)
    {
        std::string key = rand_str_scope(re, kKeyMinLen, kKeyMaxLen);
        std::string val = rand_str_scope(re, kValMinLen, kValMaxLlen);

        if (sample_cnt < sample_key_num)
        {
            samples.push_back(key);
            ++sample_cnt;
        }

        auto [it_map, inserted] = 
            key_vals_.insert({HeapKey(std::make_unique<std::string>(std::move(key))), CombinedVal(std::move(val))});
        if (inserted)
        {
            const std::string* key = &(it_map->first.real_key()); 
            if (protected_cnt < kProtectSpace)
            {
                // insert to protected list
                auto it_list = protected_list_.insert(protected_list_.end(), key);
                it_map->second.set_list_iter(true, it_list);
                ++protected_cnt;
            }
            else
            {
                // insert to probationary lisst
                auto it_list = probationary_list_.insert(probationary_list_.end(), key);
                it_map->second.set_list_iter(false, it_list);
            }
        }
    }
}

std::string* ShareData::find_val(const std::string& key)
{
    const HeapKey stack_key(&key);

    std::lock_guard<std::mutex> lk(mutex_);

    const auto it = key_vals_.find(stack_key);  // hash find

    // std::unique_ptr<std::string> s2 = std::make_unique<std::string>(key);
    // HeapKey key2(std::move(s2));
    // const auto it = key_vals_.find(key2);
    // const auto it = key_vals_.find(stack_key);
    // return it == key_vals_.end() ? nullptr : &it->second.val;

    if (it == key_vals_.end())
    {
        return nullptr;
    }
    else
    {
        bool is_protect = it->second.is_protected;
        auto it_hit = it->second.it_list;

        if (is_protect)
        {
            // if it_hit happens in protection 
            // promote it_hit to the warmest in protection
            protected_list_.splice(protected_list_.end(), protected_list_, it_hit);
        }
        else
        {
            // else it_hit happens in probation
            if (protected_list_.size() < kProtectSpace)
            {
                // if protection is not full
                // promote it_list from probation to the coldest in protection
                protected_list_.splice(protected_list_.begin(), probationary_list_, it_hit);
            }
            else
            {
                // else protection is full
                // demote the coldest in protection to the warmest in probation
                auto it_coldest_in_protect = protected_list_.begin();
                assert(it_coldest_in_protect != protected_list_.end());
                probationary_list_.splice(probationary_list_.end(), protected_list_, it_coldest_in_protect);
                // then promote it_hit from probation to the coldest in protection
                protected_list_.splice(protected_list_.begin(), probationary_list_, it_hit);
            }
        }

        return &it->second.val;
    }
}

float ShareData::hash_table_load_factor() const
{
    std::lock_guard<std::mutex> lk(mutex_);
    return key_vals_.load_factor();
}

float ShareData::max_hash_table_load_factor() const
{
    std::lock_guard<std::mutex> lk(mutex_);
    return key_vals_.max_load_factor();
}

Multi::Multi(std::shared_ptr<ShareData> data, const std::vector<std::string> samples)
    : re_(std::time(0)), data_(data), samples_(samples), hit_cnt_(0), miss_cnt_(0)
{
    for (size_t i = 0; i != samples.size(); ++i)
    {
        std::string rand_key = rand_str_scope(re_, kKeyMinLen, kValMaxLlen);
        rand_keys_.push_back(std::move(rand_key));
    }
}

Multi::~Multi()
{
    try
    {
        if (thread_.joinable())
            thread_.join();
    }
    catch (std::system_error& e)
    {
        std::cerr << "~Multi() when thread join failed, reason = " << e.what() << '\n';
    }
}

void Multi::benchmark(const size_t num)
{
    time_start_ = std::chrono::high_resolution_clock::now();

    const int hot_total = static_cast<int>(samples_.size());
    const int rand_total = static_cast<int>(rand_keys_.size());

    for (size_t i = 0; i != num; ++i)
    {
        const std::string *key;
        const int dice = re_.rand_int_scope(0, 100);
        if (dice < kHotHit)
        {
            const int index = re_.rand_int_scope(0, hot_total);
            key = &samples_.at(index);
        }
        else
        {
            // random key
            const int index = re_.rand_int_scope(0, rand_total);
            key = &rand_keys_.at(index);
        }

        const std::string* res = data_->find_val(*key);

        if (res != nullptr)
        {
            ++hit_cnt_;
        }
        else
        {
            ++miss_cnt_;
        }
    }

    time_end_ = std::chrono::high_resolution_clock::now();
}

std::chrono::milliseconds Multi::duration() const
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(time_end_ - time_start_);
}

std::tuple<std::chrono::high_resolution_clock::time_point, std::chrono::high_resolution_clock::time_point>
Multi::get_time_points() const
{
    return {time_start_, time_end_};
}

int Multi::miss_percent() const
{
    const size_t total = hit_cnt_ + miss_cnt_;

    return total == 0 ? 0 : static_cast<int>(miss_cnt_ * 100 / total);
}

void Multi::start_bench_in_thread(const size_t num)
{
    std::thread t(&Multi::benchmark, this, num);
    thread_ = std::move(t);
}

void Multi::wait_until_thread_finish()
{
    assert(thread_.joinable());
    thread_.join();
}

}   // namespace cmp_mem_engine