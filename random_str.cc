#include "random_str.h"
#include <limits>
#include <cassert>

namespace cmp_mem_engine
{

RandomEngine::RandomEngine(const size_t seed) : 
                generator_byte_(seed), 
                distribute_byte_(0, 255),
                generator_int_(seed),
                distribute_int_(std::numeric_limits<int>::min(), std::numeric_limits<int>::max()),
                generator_size_(seed),
                distribute_size_(std::numeric_limits<size_t>::min(), std::numeric_limits<size_t>::max())
{}

int RandomEngine::rand_byte()
{
    return distribute_byte_(generator_byte_);
}

int RandomEngine::rand_int()
{
    return distribute_int_(generator_int_);
}

int RandomEngine::rand_int_scope(const int min, const int max)
{
    assert(min < max);
    const int r = rand_int();
    const int div = max - min;
    
    int mod = r % div;
    if (mod < 0)
        mod += div;

    return min + mod;
}

size_t RandomEngine::rand_size_scope(const size_t min, const size_t max)
{
    assert(min < max);
    const size_t r = rand_size();
    return min + (r % (max - min));
}

size_t RandomEngine::rand_size()
{
    return distribute_size_(generator_size_);
}


std::string rand_str(RandomEngine& re, const size_t len)
{
    std::string s;
    s.reserve(len);

    for (size_t i = 0; i != len; ++i)
    {
        const int byte = re.rand_byte();
        s.push_back(static_cast<char>(byte));
    }

    return s;
}

std::string rand_str_scope(RandomEngine& re, const size_t min_len, const size_t max_len)
{
    assert(min_len < max_len);

    const size_t len = re.rand_size_scope(min_len, max_len);
    return rand_str(re, len);
}

}   // namespace cmp_mem_engine
