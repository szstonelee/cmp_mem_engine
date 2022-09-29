#include "single_thread.h"

#include <cassert>
#include <memory>
#include <ctime>

#include "const_and_share_struct.h"

namespace cmp_mem_engine
{

Single::Single(const size_t init_key_num, const size_t hot_key_num, const size_t rand_key_num)
    : re_(std::time(0)), found_val_cnt_(0)
{
    assert(hot_key_num <= init_key_num);

    data_ = std::make_unique<SingleData>(init_key_num, hot_key_num, hot_keys_);

    for (size_t i = 0; i != rand_key_num; ++i)
    {
        std::string key = rand_str_scope(re_, kKeyMinLen, kKeyMaxLen);
        rand_keys_.push_back(std::move(key));
    }
}

std::string* Single::find_val(const std::string& key)
{
    return data_->find_val(key);
}

void Single::bench_lookup()
{
    const int hot_total = static_cast<int>(hot_keys_.size());
    const int rand_total = static_cast<int>(rand_keys_.size());

    std::string *key;
    const int dice = re_.rand_int_scope(0, 100);
    if (dice < kHotHit)
    {
        const int index = re_.rand_int_scope(0, hot_total);
        key = &hot_keys_.at(index);
    }
    else
    {
        // random key
        const int index = re_.rand_int_scope(0, rand_total);
        key = &rand_keys_.at(index);
    }

    const std::string* val = find_val(*key);

    if (val != nullptr)
        ++found_val_cnt_;   // try to use val, otherwise compiler maybe optimize
}

void Single::benchmark()
{
    for (size_t i = 0; i != kBenchmarkCount; ++i)
    {
        bench_lookup();
    }
}

int Single::miss_percent() const
{
    auto [hit, miss] = data_->hit_miss();

    const size_t total = hit + miss;

    return total == 0 ? 0 : static_cast<int>(miss * 100 / total);
}

}   // namespace cmp_mem_engine