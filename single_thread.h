#pragma once

#include <unordered_map>
#include <string>
#include <vector>
#include <memory>
#include <list>

#include "const_and_share_struct.h"
#include "random_str.h"


namespace cmp_mem_engine
{

class Single
{
private:
    RandomEngine re_;
    std::unique_ptr<SingleData> data_;

    std::vector<std::string> hot_keys_;
    std::vector<std::string> rand_keys_;

    size_t found_val_cnt_;

public:
    Single() = delete;
    Single& operator=(const Single& copy) = delete;

    /* install at most init_key_num to key_vals_, 
     * and sample at most hot_key_num keys in hot_keys (NOTE: can be duplicated) */
    explicit Single(const size_t init_key_num, const size_t hot_key_num, const size_t rand_key_num);

    void benchmark();
    int miss_percent() const;

private:
    std::string* find_val(const std::string& key);
    void bench_lookup();
};

}   // namespace cmp_mem_engine