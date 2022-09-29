#pragma once

#include <cstddef>
#include <string>
#include <list>
#include <memory>
#include <cassert>
#include <unordered_map>
#include <jemalloc/jemalloc.h>
#include <new>
#include <atomic>
#include <string>
#include <new>

#include "random_str.h"


#ifdef __cpp_lib_hardware_interference_size
    using std::hardware_constructive_interference_size;
    using std::hardware_destructive_interference_size;
#else
    // 64 bytes on x86-64 │ L1_CACHE_BYTES │ L1_CACHE_SHIFT │ __cacheline_aligned │ ...
    constexpr std::size_t hardware_constructive_interference_size = 64;
    constexpr std::size_t hardware_destructive_interference_size = 64;
#endif


// #define USE_SPINLOCK_FOR_TASKS      // please use for pure only for compare spinlock and mutext difference

namespace cmp_mem_engine
{

constexpr size_t kKeyMinLen = 2;
constexpr size_t kKeyMaxLen = 64;
constexpr size_t kValMinLen = 20;
constexpr size_t kValMaxLlen = 2000;

constexpr size_t kKeySpace = 1<<20;
constexpr size_t kHotSpace = 1<<10;
constexpr size_t kRandSpace = 1<<12;
constexpr size_t kSampleSpace = 1<<12;

constexpr size_t kProtectSpace = kKeySpace * 90 / 100;

constexpr int kHotHit = 90;

constexpr size_t kBenchmarkCount = 1<<24;

constexpr size_t kPidZeroMeaningEmpty = 0;
constexpr size_t kPidMaxMeaninngExit = std::numeric_limits<size_t>::max();
constexpr size_t kTaskLen = 64;

extern const char* kNotFound;

constexpr size_t kTransactionOneStepLeastKeys = 1;
constexpr size_t kTransactionOneStepMostKeys = 20;

static_assert(kTransactionOneStepLeastKeys <= kTransactionOneStepMostKeys);

constexpr size_t kFixProducerNumber = 8;
constexpr size_t kRunProducerNum = 2;
static_assert(kRunProducerNum > 0 && kRunProducerNum <= kFixProducerNumber);

extern const char* kNotFound;
extern const char* kExitConsumerThreadTask;

constexpr size_t kLockLessArrayNum = 1 * (hardware_destructive_interference_size/sizeof(std::atomic<std::string*>));

struct CombinedVal
{
    CombinedVal(std::string&& _val) : val(std::move(_val)), is_protected(true)
    {}

    void set_list_iter(const bool _is_protected, std::list<const std::string*>::iterator _it_list)
    {
        is_protected = _is_protected;
        it_list = _it_list;
    }

    std::string val;

    bool is_protected;
    std::list<const std::string*>::iterator it_list;
};

class HeapKey
{
public:
    HeapKey() = delete;
    HeapKey& operator=(const HeapKey&) = delete;
    HeapKey& operator=(HeapKey&&) = delete;
    HeapKey(HeapKey&&) = delete;

    HeapKey(std::unique_ptr<std::string>&& wrap_key)
        : wrap_key_(std::move(wrap_key)), key_in_stack_(false)
    {}

    /* Use for stack key only, avoid allocation of memory, usually use for find() */
    HeapKey(const std::string* stack_key) 
        : wrap_key_(const_cast<std::string*>(stack_key)), key_in_stack_(true)
    {}
    
    HeapKey(const HeapKey& copy)
    {
        if (copy.key_in_stack_)
        {
            key_in_stack_ = true;
            // wrap_key_.release();
            wrap_key_.reset(copy.wrap_key_.get());
        }
        else
        {
            key_in_stack_ = false;
            wrap_key_ = std::move(const_cast<HeapKey&>(copy).wrap_key_);
        }
    }

    ~HeapKey()
    {
        if (key_in_stack_)
        {
            wrap_key_.release();    // do not destory the key because it is in stack
        }
    }

    const std::string& real_key() const
    {
        return *wrap_key_;
    }

    bool operator==(const HeapKey& other) const
    {
        return this->real_key() == other.real_key();
    }

private:
    std::unique_ptr<const std::string>  wrap_key_;

    bool key_in_stack_; 
};

}   // namespace cmp_mem_engine

namespace std
{
    template<>
    struct hash<cmp_mem_engine::HeapKey>
    {
        std::size_t operator()(const cmp_mem_engine::HeapKey& heap_key) const
        {
            return std::hash<std::string>()(heap_key.real_key());
        }
    };
}

namespace cmp_mem_engine
{

// Own by one single threead, no lock using
class SingleData
{
private:
    RandomEngine re_;

    std::unordered_map<HeapKey, CombinedVal> key_vals_;
    // lists of cold(head) -> warm(tail)
    std::list<const std::string*> protected_list_;
    std::list<const std::string*> probationary_list_;

    size_t hit_cnt_;
    size_t miss_cnt_;

public:
    SingleData() = delete;
    SingleData(const SingleData&) = delete;
    SingleData(SingleData&&) = delete;
    SingleData& operator=(const SingleData&) = delete;
    SingleData& operator=(SingleData&&) = delete;

    explicit SingleData(const size_t init_key_num, const size_t sample_num, std::vector<std::string>& samples)
        : re_(1), hit_cnt_(0), miss_cnt_(0)
    {
        assert(sample_num <= init_key_num && samples.empty());

        key_vals_.reserve(init_key_num);

        size_t sample_cnt = 0, protected_cnt = 0;

        for (size_t i = 0; i != init_key_num; ++i)
        {
            std::string key = rand_str_scope(re_, kKeyMinLen, kKeyMaxLen);
            std::string val = rand_str_scope(re_, kValMinLen, kValMaxLlen);

            if (sample_cnt < sample_num)
            {
                samples.push_back(key);
                ++sample_cnt;
            }
            
            // add key and value to HashMap and 2Q list
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

    // Return nullptr if not found, else the value of std::sttring.
    // It will refresh the 2Q list for each lookup
    std::string* find_val(const std::string& key)
    {
        const HeapKey stack_key(&key);      // construct a stack-memory(pseduo) HeapKey

        const auto it = key_vals_.find(stack_key);  // hash find

        if (it == key_vals_.end())
        {
            ++miss_cnt_;

            return nullptr;
        }
        else
        {
            ++hit_cnt_;

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

    std::tuple<size_t, size_t> hit_miss() const
    {
        return {hit_cnt_, miss_cnt_};
    }
};

}   // cmp_mem_engine