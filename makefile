cmp:
	g++ -O3 -std=c++17 -Wall -Wextra -fsanitize=leak cmp.cc pc_lockless.cc pc_pure.cc pc_signal.cc producer_consumer.cc multi_threads.cc single_thread.cc random_str.cc -ljemalloc -lpthread

