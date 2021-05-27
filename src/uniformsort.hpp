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

#include "./disk.hpp"
#include "./util.hpp"

namespace UniformSort {

    inline int64_t const BUF_SIZE = 262144;
    std::vector<uint8_t> ZERO_MEM(1024, 0);
    inline static bool IsPositionEmpty(const uint8_t *memory, uint32_t const entry_len)
    {
        return !memcmp(memory, ZERO_MEM.data(), entry_len);
    }



    inline void SortToMemory2(
        FileDisk &input_disk,
        uint64_t const input_disk_begin,
        uint8_t *const memory,
        uint32_t const entry_len,
        uint64_t const num_entries,
        uint32_t const bits_begin)
    {
        uint64_t const memory_len = Util::RoundSize(num_entries) * entry_len;
        auto const swap_space = std::make_unique<uint8_t[]>(entry_len);
        auto const buffer = std::make_unique<uint8_t[]>(BUF_SIZE);
        uint64_t bucket_length = 0;
        // The number of buckets needed (the smallest power of 2 greater than 2 * num_entries).
        while ((1ULL << bucket_length) < 2 * num_entries) bucket_length++;
        memset(memory, 0, memory_len);

        uint64_t read_pos = input_disk_begin;
        uint64_t buf_size = 0;
        uint64_t buf_ptr = 0;
        uint64_t swaps = 0;
        for (uint64_t i = 0; i < num_entries; i++) {
            if (buf_size == 0) {
                // If read buffer is empty, read from disk and refill it.
                buf_size = std::min((uint64_t)BUF_SIZE / entry_len, num_entries - i);
                buf_ptr = 0;
                input_disk.Read(read_pos, buffer.get(), buf_size * entry_len);
                read_pos += buf_size * entry_len;
            }
            buf_size--;
            // First unique bits in the entry give the expected position of it in the sorted array.
            // We take 'bucket_length' bits starting with the first unique one.
            uint64_t pos =
                Util::ExtractNum(buffer.get() + buf_ptr, entry_len, bits_begin, bucket_length) *
                entry_len;
            // As long as position is occupied by a previous entry...
            while (!IsPositionEmpty(memory + pos, entry_len) && pos < memory_len) {
                // ...store there the minimum between the two and continue to push the higher one.
                if (Util::MemCmpBits(
                        memory + pos, buffer.get() + buf_ptr, entry_len, bits_begin) > 0) {
                    memcpy(swap_space.get(), memory + pos, entry_len);
                    memcpy(memory + pos, buffer.get() + buf_ptr, entry_len);
                    memcpy(buffer.get() + buf_ptr, swap_space.get(), entry_len);
                    swaps++;
                }
                pos += entry_len;
            }
            // Push the entry in the first free spot.
            memcpy(memory + pos, buffer.get() + buf_ptr, entry_len);
            buf_ptr += entry_len;
        }
        uint64_t entries_written = 0;
        std::vector<uint64_t> debug;
        // Search the memory buffer for occupied entries.
        for (uint64_t pos = 0; entries_written < num_entries && pos < memory_len;
             pos += entry_len) {
            if (!IsPositionEmpty(memory + pos, entry_len)) {
                // We've found an entry.
                // write the stored entry itself.
                memcpy(
                    memory + entries_written * entry_len,
                    memory + pos,
                    entry_len);
                debug.push_back(
                    Util::ExtractNum(memory + pos, entry_len, bits_begin, bucket_length));
                entries_written++;
            }
        }

        std::cout << "Memlen: " << memory_len << ", entry num: " << num_entries
                  << ", entry len: " << entry_len << ", entry mem: " << entry_len * num_entries << ", Entry write: " << entries_written
                  << "\n";
        std::cout << "Debug[RA]: ";
        for (size_t i = 0; i < 50; ++i) {
            std::cout << std::setw(8) << debug[i] << ", ";
        }
        std::cout << " ... ";
        for (size_t i = debug.size() - 50; i < debug.size(); ++i) {
            std::cout << std::setw(8) << debug[i] << ", ";
        }
        std::cout << "\n";
        assert(entries_written == num_entries);
    }


    inline void SortToMemory(
        FileDisk &input_disk,
        uint64_t const input_disk_begin,
        uint8_t *const memory,
        uint32_t const entry_len,
        uint64_t const num_entries,
        uint32_t const bits_begin)
    {


        uint64_t const memory_len = Util::RoundSize(num_entries) * entry_len;
        auto const swap_space = std::make_unique<uint8_t[]>(entry_len);
        auto const buffer = std::make_unique<uint8_t[]>(BUF_SIZE);
        uint64_t bucket_length = 0;


        // SortToMemory2(input_disk, input_disk_begin, memory, entry_len, num_entries, bits_begin);
        // uint8_t* mem_cpy = new uint8_t[memory_len];
        // memcpy(mem_cpy, memory, memory_len);

        // The number of buckets needed (the smallest power of 2 greater than 2 * num_entries).
        while ((1ULL << bucket_length) < 2 * num_entries) bucket_length++;
        memset(memory, 0, memory_len);

        uint64_t read_pos = input_disk_begin;
        uint64_t buf_ptr = 0;
        std::vector<uint64_t> pos_vec;
        assert(num_entries * entry_len < memory_len);
        input_disk.Read(read_pos, memory, std::min(num_entries * entry_len, memory_len));
       
        uint8_t *cpy = new uint8_t[memory_len];
        memcpy(cpy, memory, memory_len); 
        
        for (uint64_t i = 0; i < num_entries; i++) {
            uint64_t pos = Util::ExtractNum(memory + buf_ptr, entry_len, bits_begin, bucket_length);
            pos_vec.push_back(pos);
            buf_ptr += entry_len;
        }

        // pos_vec.resize(100);
        std::vector<size_t> idx(num_entries);
        std::iota(idx.begin(), idx.end(), 0);

        std::stable_sort(idx.begin(), idx.end(), [&](size_t i1, size_t i2) {
            return Util::MemCmpBits(
                       memory + i1 * entry_len, memory + i2 * entry_len, entry_len, bits_begin) < 0;
        });
        // return pos_vec[i1] <= pos_vec[i2];});

        // std::map<size_t, size_t> re2;
        // for(size_t i = 0; i < idx.size(); ++i) {
        //     re2[i] = idx[i];
        // }

        // std::cout << "Debug[II]: ";
        // for (uint64_t i = 0; i < std::min(uint64_t(100), num_entries); ++i) {
        //     std::cout << std::setw(8) << i << ", ";
        // }
        // std::cout << "\nDebug[RA]: ";
        // for (uint64_t i = 0; i < std::min(uint64_t(100), num_entries); ++i) {
        //     std::cout << std::setw(8) << pos_vec[i] << ", ";
        // }
        // std::cout << "\nDebug[ID]: ";
        // for (uint64_t i = 0; i < std::min(uint64_t(100), num_entries); ++i) {
        //     std::cout << std::setw(8) << idx[i] << ", ";
        // }


        for (uint64_t i = 0; i < num_entries; ++i) {
            int cur = i;
            while (i != idx[cur] && idx[cur] < idx.size()) {
                std::swap_ranges(memory + cur * entry_len, memory + cur * entry_len + entry_len, memory + idx[cur] * entry_len);
                int tmp = cur;
                cur = idx[cur];
                idx[tmp] = idx.size();
            }
            idx[cur] = idx.size();
        }

        // std::cout << "\nDebug[QS]: " ;
        // for (uint64_t i = 0; i < 50; ++i) {
        //     std::cout << std::setw(8) << Util::ExtractNum(memory + i * entry_len, entry_len, bits_begin, bucket_length) << ", ";
        // }
        // std::cout << " ... ";
        // for (uint64_t i = num_entries - 50; i < num_entries; ++i) {
        //     std::cout << std::setw(8) << Util::ExtractNum(memory + i * entry_len, entry_len, bits_begin, bucket_length) << ", ";
        // }
        // std::cout << std::endl;
        // for(uint64_t i = 0; i < num_entries; ++i) {
        //     if (0 != memcmp(mem_cpy + i * entry_len, memory + i * entry_len, entry_len)) {
        //         int a = Util::ExtractNum(mem_cpy + i * entry_len, entry_len, bits_begin, bucket_length);
        //         int b = Util::ExtractNum(memory + i * entry_len, entry_len, bits_begin, bucket_length);
        //         int r = re2[i];
        //         int c = Util::ExtractNum(cpy + r * entry_len, entry_len, bits_begin, bucket_length);
        //         std::cout << "Found diff. " << i << ", pos1: " << a << ", pos2: " << b << ", raw: " << c << std::endl;
        //         for(uint32_t j = 0; j < entry_len; ++j) {
        //             std::cout <<  std::setw(2) << *(uint8_t*)(mem_cpy + i * entry_len + j) << " "
        //                       <<  std::setw(2) << *(uint8_t*)(memory + i * entry_len + j) << " "
        //                       <<  std::setw(2) << *(uint8_t*)(cpy + r * entry_len + j) << "\n";
        //         }
        //     }
        // }

        return;
    }
}

#endif  // SRC_CPP_UNIFORMSORT_HPP_
