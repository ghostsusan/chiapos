// Copyright 2018 Chia Network Inc

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//    http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef SRC_CPP_UNIFORMSORT_HPP_
#define SRC_CPP_UNIFORMSORT_HPP_

#include <algorithm>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <future>
#include <atomic>

#include "./disk.hpp"
#include "./util.hpp"

namespace UniformSort {

class SpinLock {
public:
    SpinLock() : flag_(false) {}

    void lock()
    {
        bool expect = false;
        while (!flag_.compare_exchange_weak(expect, true))
        {
            expect = false;
        }
    }

    void unlock()
    {
        flag_.store(false);
    }

private:
    std::atomic<bool> flag_;
};

    inline int64_t const BUF_SIZE = 262144;
    std::vector<uint8_t> ZERO_MEM(1024, 0);
    inline static bool IsPositionEmpty(const uint8_t *memory, uint32_t const entry_len)
    {
        return !memcmp(memory, ZERO_MEM.data(), entry_len);
    }



    inline void SortToMemory(
        FileDisk &input_disk,
        uint64_t const input_disk_begin,
        uint8_t *const memory,
        uint32_t const entry_len,
        uint64_t const num_entries,
        uint32_t const bits_begin,
        uint32_t * idx_arr)
    {
        uint64_t bucket_length = 0;
        // The number of buckets needed (the smallest power of 2 greater than 2 * num_entries).
        while ((1ULL << bucket_length) < 2 * num_entries) bucket_length++;

        uint64_t buf_ptr = 0;
        auto round_size = Util::RoundSize(num_entries);

        for (uint64_t i = 0; i < num_entries; i++) {
            uint64_t position = Util::ExtractNum(memory + buf_ptr, entry_len, bits_begin, bucket_length);
            uint32_t swap = i;

            // auto ptr = __sync_val_compare_and_swap(&idx_arr[position], 0xFFFFFFFF, swap);
            // // Failed to swap
            // if (ptr == &idx_arr[position]) {
            //     buf_ptr += entry_len;
            //     continue;
            // }

            // spinlock.lock();
            while (idx_arr[position] != 0xFFFFFFFF && position < round_size) {
                // std::cout << "Pos: " << std::dec << position << ", swap: " << swap << ", idx_arr: " << std::hex << idx_arr[position] << std::endl;
                if (Util::MemCmpBits( memory + idx_arr[position] * entry_len, memory + swap * entry_len, entry_len, bits_begin) > 0) {
                    std::swap(idx_arr[position], swap);
                } 
                ++position;
            }
            idx_arr[position] = swap;
            // spinlock.unlock();
            buf_ptr += entry_len;
        }
        uint64_t entries_written = 0;
        for (size_t i = 0; entries_written < num_entries; ++i) {
            if (idx_arr[i] != 0xFFFFFFFF) {
                idx_arr[entries_written++] = idx_arr[i];
            }
        }
        assert(entries_written == num_entries);
    }

}

#endif  // SRC_CPP_UNIFORMSORT_HPP_
