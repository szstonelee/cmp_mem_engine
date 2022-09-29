#pragma once

#include <random>

/* Each thred need new and own only one RandomEngine for itselef 
 * before call the API
 * (usually wrapped by unique_ptr).
 * When calling random string API, we need the RandomEngine as argument.  
 */

namespace cmp_mem_engine
{

class RandomEngine
{
public:
    RandomEngine(const size_t seed);

    int rand_byte();

    int rand_int();

    /* [min, max), min < max */
    int rand_int_scope(const int min, const int max);

    size_t rand_size();

    /* [min, max), min < max */
    size_t rand_size_scope(const size_t min, const size_t max);

private:
    // std::random_device device_byte_;
    std::ranlux24_base generator_byte_;
    std::uniform_int_distribution<int> distribute_byte_;

    // std::random_device device_int_;
    std::ranlux24_base generator_int_;
    std::uniform_int_distribution<int> distribute_int_;

    // std::random_device device_size_;
    std::ranlux48_base generator_size_;
    std::uniform_int_distribution<size_t> distribute_size_;
};

std::string rand_str(RandomEngine& re, const size_t len);
/* string lenght is random in [min_len, max_len), min_len < max_len */
std::string rand_str_scope(RandomEngine& re, const size_t min_len, const size_t max_len);

}   // namespace cmp_mem_engine
