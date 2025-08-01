CPFLAGS = -g -Wall -O3 -std=c++17 -lpmem -pthread -Ofast

MapEmbed: main.cpp murmur3.h MapEmbed_pmem.h MapEmbed_pmem_thread.h
	g++ -mavx2 main.cpp -o MapEmbed $(CPFLAGS)

clean: 
	rm MapEmbed
