#include <iostream>
#include <chrono>
#include <thread>
#include <cassert>

// Test result show Linux and MacOS need less than 100us timer resolution
// For long sleep, there is 100us more cost for 1ms or 10ms sleep

constexpr size_t kNumMicrosPerSecond = 1000000;

enum class SleepMethod
{
    kSleepFor, kNanoSleep,
};

void test_sleep(const SleepMethod sleep_method, const size_t micros)
{
    using namespace std::chrono_literals;

    std::chrono::high_resolution_clock::time_point start = std::chrono::high_resolution_clock::now();

    int cnt = 0;
    while (cnt != 100'000)
    {
        switch (sleep_method)
        {
        case SleepMethod::kNanoSleep:
            struct timespec sleep_time;
            sleep_time.tv_sec = micros/ kNumMicrosPerSecond;
            sleep_time.tv_nsec = (micros % kNumMicrosPerSecond) * 1000;
            int ret;
            ret = nanosleep(&sleep_time, &sleep_time);
            assert(ret == 0);
            break;

        case SleepMethod::kSleepFor:
            std::this_thread::sleep_for(micros*1us);
            break;

        default:
            assert(0);
        }
        
        ++cnt;
    }

    std::chrono::high_resolution_clock::time_point end = std::chrono::high_resolution_clock::now();
    std::chrono::microseconds duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

    std::cout << "SleepMothod = " << (sleep_method == SleepMethod::kSleepFor ? "kSleepFor" : "kNanoSleep")
              << "total repeat times = " << cnt 
              << ", elasped time(ms) = " << duration.count() / 1000
              << ", one avg sleep(us) = " << duration.count() / cnt
              << '\n';
}

int main()
{
    test_sleep(SleepMethod::kSleepFor, 10);

    test_sleep(SleepMethod::kNanoSleep, 10);

    return 0;
}