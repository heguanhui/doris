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

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include "common/config.h"
#include "core/data_type/data_type_bitmap.h"
#include "core/data_type/data_type_string.h"
#include "core/types.h"
#include "core/value/bitmap_value.h"
#include "exprs/function/function_test_util.h"
#include "util/url_coding.h"

namespace doris {

namespace config {
DECLARE_Bool(enable_set_in_bitmap_value);
}

static std::string bitmap_to_expected_base64(const BitmapValue& bitmap) {
    size_t ser_size = bitmap.getSizeInBytes();
    std::string ser_buf(ser_size, '\0');
    bitmap.write_to(ser_buf.data());

    std::string encoded;
    base64_encode(std::string(ser_buf.data(), ser_size), &encoded);
    return encoded;
}

TEST(BitmapToBase64OffsetTest, MultipleBitmapsCorrectOffsets) {
    config::Register::Field field("bool", "enable_set_in_bitmap_value",
                                  &config::enable_set_in_bitmap_value, "false", false);
    config::Register::_s_field_map->insert(
            std::make_pair(std::string("enable_set_in_bitmap_value"), field));

    EXPECT_TRUE(config::set_config("enable_set_in_bitmap_value", "false", false, true).ok());

    std::string func_name = "bitmap_to_base64";
    InputTypeSet input_types = {PrimitiveType::TYPE_BITMAP};

    BitmapValue bitmap1(1);
    BitmapValue bitmap2({1, 9999999});
    BitmapValue bitmap3;
    BitmapValue bitmap4((uint64_t)4294967296);

    DataSet data_set = {
            {{&bitmap1}, bitmap_to_expected_base64(bitmap1)},
            {{&bitmap2}, bitmap_to_expected_base64(bitmap2)},
            {{&bitmap3}, bitmap_to_expected_base64(bitmap3)},
            {{&bitmap4}, bitmap_to_expected_base64(bitmap4)},
    };

    static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
}

TEST(BitmapToBase64OffsetTest, ManyBitmapsNoTrailingGarbage) {
    config::Register::Field field("bool", "enable_set_in_bitmap_value",
                                  &config::enable_set_in_bitmap_value, "false", false);
    config::Register::_s_field_map->insert(
            std::make_pair(std::string("enable_set_in_bitmap_value"), field));

    EXPECT_TRUE(config::set_config("enable_set_in_bitmap_value", "false", false, true).ok());

    std::string func_name = "bitmap_to_base64";
    InputTypeSet input_types = {PrimitiveType::TYPE_BITMAP};

    DataSet data_set;
    for (int i = 0; i < 50; ++i) {
        BitmapValue bitmap(i);
        data_set.push_back({{&bitmap}, bitmap_to_expected_base64(bitmap)});
    }

    static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
}

} // namespace doris
