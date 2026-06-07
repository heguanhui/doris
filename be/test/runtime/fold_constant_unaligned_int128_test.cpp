// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <gtest/gtest.h>

#include <cstring>
#include <vector>

#include "core/value/large_int_value.h"
#include "util/unaligned.h"

namespace doris {

static char* misaligned_slot(std::vector<char>& buf, std::size_t bytes) {
    buf.assign(bytes + 32, 0);
    char* base = buf.data();
    std::size_t off = 0;
    while ((reinterpret_cast<std::uintptr_t>(base + off) & 0xF) != 1) {
        ++off;
    }
    return base + off;
}

TEST(FoldConstantUnalignedInt128Test, LargeIntToStringFromUnalignedBuffer) {
    std::vector<char> buf;
    char* p = misaligned_slot(buf, sizeof(__int128));
    ASSERT_NE(reinterpret_cast<std::uintptr_t>(p) % alignof(__int128), 0u);

    __int128 expected = (static_cast<__int128>(0x3FFFFFFFFFFFFFFFLL) << 64) |
                        static_cast<__int128>(0xFEEDFACECAFEBEEFULL);
    std::memcpy(p, &expected, sizeof(expected));

    std::string result = LargeIntValue::to_string(unaligned_load<__int128>(p));
    EXPECT_EQ(result, LargeIntValue::to_string(expected));
}

TEST(FoldConstantUnalignedInt128Test, NegativeLargeIntFromUnalignedBuffer) {
    std::vector<char> buf;
    char* p = misaligned_slot(buf, sizeof(__int128));
    ASSERT_NE(reinterpret_cast<std::uintptr_t>(p) % alignof(__int128), 0u);

    __int128 expected = -(static_cast<__int128>(0x1A2B3C4D5E6F7A8BLL) << 64) -
                        static_cast<__int128>(0x1122334455667788LL);
    std::memcpy(p, &expected, sizeof(expected));

    std::string result = LargeIntValue::to_string(unaligned_load<__int128>(p));
    EXPECT_EQ(result, LargeIntValue::to_string(expected));
}

TEST(FoldConstantUnalignedInt128Test, ZeroFromUnalignedBuffer) {
    std::vector<char> buf;
    char* p = misaligned_slot(buf, sizeof(__int128));
    ASSERT_NE(reinterpret_cast<std::uintptr_t>(p) % alignof(__int128), 0u);

    __int128 expected = 0;
    std::memcpy(p, &expected, sizeof(expected));

    std::string result = LargeIntValue::to_string(unaligned_load<__int128>(p));
    EXPECT_EQ(result, "0");
}

TEST(FoldConstantUnalignedInt128Test, MaxInt128FromUnalignedBuffer) {
    std::vector<char> buf;
    char* p = misaligned_slot(buf, sizeof(__int128));
    ASSERT_NE(reinterpret_cast<std::uintptr_t>(p) % alignof(__int128), 0u);

    __int128 expected = MAX_INT128;
    std::memcpy(p, &expected, sizeof(expected));

    std::string result = LargeIntValue::to_string(unaligned_load<__int128>(p));
    EXPECT_EQ(result, "170141183460469231731687303715884105727");
}

} // namespace doris
