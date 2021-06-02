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

#ifndef SRC_CPP_QUICKSORT_HPP_
#define SRC_CPP_QUICKSORT_HPP_

#include <algorithm>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <memory>
#include <thread>
#include <future>

#include "util.hpp"


// MaskLen: BitBegin + NumBucketLog
//   K: [20, 36]
//   Log2(K): {4,5} 
//   phase1: BitBegin: 0
//           NumBucketLog: Log2(K) 
//   phase2: BitBegin: K          
//           NumBucketLog: Log2(K) 
//   phase3: BitBegin: 0           
//           NumBucketLog: Log2(K)
//   phase4: BitBegin: 0
//           NumBucketLog: Log2(K)
// EntryLen: 
//   phase1: 
//     GetKeyPosOffsetSize(K)       {7,8,9,10}
//     GetMaxEntrySize(k, 1, true); {7,8,9,10}
//   phase2:
//     GetKeyPosOffsetSize(k);      {7,8,9,10}
//   phase3:
//     GetMaxEntrySize(k, table_index + 1, false);  {20..30}
//     cdiv(2 * k + (table_index == 6 ? 1 : 0), 8); {6,7,8,9,10}
//   phase4:
//     

namespace QuickSort {
    inline static void SortInner2(
        uint8_t *memory,
        uint64_t memory_len,
        uint32_t L,
        uint32_t bits_begin,
        uint64_t begin,
        uint64_t end,
        uint32_t *idx_arr)
    {
        size_t pivot;
        if (end - begin <= 5) {

            for (uint64_t i = begin + 1; i < end; i++) {
                uint64_t j = i;
                uint32_t pivot = idx_arr[i]; 
                while (j > begin &&
                       Util::MemCmpBits(memory + idx_arr[j - 1] * L, memory + pivot * L, L, bits_begin) > 0) {
                    idx_arr[j] = idx_arr[j - 1];
                    j--;
                }
                idx_arr[j] = pivot;
            }
            return;
        }

        uint64_t lo = begin;
        uint64_t hi = end - 1;

        pivot = idx_arr[hi];
        bool left_side = true;

        while (lo < hi) {
            if (left_side) {
                if (Util::MemCmpBits(memory + idx_arr[lo] * L, memory + pivot * L, L, bits_begin) < 0) {
                    ++lo;
                } else {
                    idx_arr[hi] = idx_arr[lo];
                    --hi;
                    left_side = false;
                }
            } else {
                if (Util::MemCmpBits(memory + idx_arr[hi] * L, memory + pivot * L , L, bits_begin) > 0) {
                    --hi;
                } else {
                    idx_arr[lo] = idx_arr[hi];
                    ++lo;
                    left_side = true;
                }
            }
        }
        idx_arr[lo] = pivot;
        if (lo - begin <= end - lo) {
            auto future = std::async(lo - begin > 65535 ? std::launch::async : std::launch::deferred, SortInner2, memory, memory_len, L, bits_begin, begin, lo, idx_arr);
            SortInner2(memory, memory_len, L, bits_begin, lo + 1, end, idx_arr);
            future.wait();
        } else {
            auto future = std::async(end - lo  > 65535 ? std::launch::async : std::launch::deferred, SortInner2, memory, memory_len, L, bits_begin, lo + 1, end, idx_arr);
            SortInner2(memory, memory_len, L, bits_begin, begin, lo, idx_arr);
            future.wait();
        }
    }

    inline void Sort2(
        uint8_t *const memory,
        uint32_t const entry_len,
        uint64_t const num_entries,
        uint32_t const bits_begin,
        uint32_t *idx_arr)
    {
        // std::cout << "EntryLen: " << entry_len << ", bits_begin: " << bits_begin << "\n";
        uint64_t const memory_len = (uint64_t)entry_len * num_entries;
        SortInner2(memory, memory_len, entry_len, bits_begin, 0, num_entries, idx_arr);
        // SortInner2(memory, memory_len, entry_len, bits_begin, 0, 1000, idx_arr);
    }

    inline static void SortInner(
        uint8_t *memory,
        uint64_t memory_len,
        uint32_t L,
        uint32_t bits_begin,
        uint64_t begin,
        uint64_t end,
        uint8_t *pivot_space)
    {
        if (end - begin <= 5) {
            for (uint64_t i = begin + 1; i < end; i++) {
                uint64_t j = i;
                memcpy(pivot_space, memory + i * L, L);
                while (j > begin &&
                       Util::MemCmpBits(memory + (j - 1) * L, pivot_space, L, bits_begin) > 0) {
                    memcpy(memory + j * L, memory + (j - 1) * L, L);
                    j--;
                }
                memcpy(memory + j * L, pivot_space, L);
            }
            return;
        }

        uint64_t lo = begin;
        uint64_t hi = end - 1;

        memcpy(pivot_space, memory + (hi * L), L);
        bool left_side = true;

        while (lo < hi) {
            if (left_side) {
                if (Util::MemCmpBits(memory + lo * L, pivot_space, L, bits_begin) < 0) {
                    ++lo;
                } else {
                    memcpy(memory + hi * L, memory + lo * L, L);
                    --hi;
                    left_side = false;
                }
            } else {
                if (Util::MemCmpBits(memory + hi * L, pivot_space, L, bits_begin) > 0) {
                    --hi;
                } else {
                    memcpy(memory + lo * L, memory + hi * L, L);
                    ++lo;
                    left_side = true;
                }
            }
        }
        memcpy(memory + lo * L, pivot_space, L);
        auto const ps = std::make_unique<uint8_t[]>(L);
        if (lo - begin <= end - lo) {
            auto future = std::async(lo - begin > 65535 ? std::launch::async : std::launch::deferred, SortInner, memory, memory_len, L, bits_begin, begin, lo, ps.get());
            SortInner(memory, memory_len, L, bits_begin, lo + 1, end, pivot_space);
            future.wait();
        } else {
            auto future = std::async(end - lo  > 65535 ? std::launch::async : std::launch::deferred, SortInner, memory, memory_len, L, bits_begin, lo + 1, end, ps.get());
            SortInner(memory, memory_len, L, bits_begin, begin, lo, pivot_space);
            future.wait();
        }
    }

    inline void Sort(
        uint8_t *const memory,
        uint32_t const entry_len,
        uint64_t const num_entries,
        uint32_t const bits_begin)
    {
        // std::cout << "EntryLen: " << entry_len << ", bits_begin: " << bits_begin << "\n";
        uint64_t const memory_len = (uint64_t)entry_len * num_entries;
        auto const pivot_space = std::make_unique<uint8_t[]>(entry_len);
        SortInner(memory, memory_len, entry_len, bits_begin, 0, num_entries, pivot_space.get());
    }


}

#endif  // SRC_CPP_QUICKSORT_HPP_
