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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gen_cpp/types.pb.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "core/block/block.h"
#include "core/column/column_decimal.h"
#include "core/column/column_ipv6.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_ipv6.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type_serde/data_type_serde.h"
#include "core/types.h"
#include "exprs/function/cast/cast_base.h"
#include "util/jsonb_document.h"
#include "util/jsonb_writer.h"

namespace doris {

namespace {

void check_pb_round_trip(const DataTypePtr& data_type, const IColumn& input_column) {
    const DataTypeSerDeSPtr serde = data_type->get_serde();
    PValues pv;
    ASSERT_TRUE(serde->write_column_to_pb(input_column, pv, 0, input_column.size()).ok());

    auto output_column = data_type->create_column();
    ASSERT_TRUE(serde->read_column_from_pb(*output_column, pv).ok());

    PValues pv2;
    ASSERT_TRUE(serde->write_column_to_pb(*output_column, pv2, 0, output_column->size()).ok());
    EXPECT_EQ(pv.bytes_value_size(), pv2.bytes_value_size());

    Block block_in, block_out;
    block_in.insert({input_column.get_ptr(), data_type, ""});
    block_out.insert({std::move(output_column), data_type, ""});
    EXPECT_EQ(block_in.dump_data(), block_out.dump_data());
}

void check_jsonb_round_trip(const DataTypePtr& data_type, const IColumn& input_column,
                            int row_num) {
    auto serde = data_type->get_serde(0);
    DataTypeSerDe::FormatOptions options;
    auto tz = cctz::utc_time_zone();
    options.timezone = &tz;

    JsonbWriterT<JsonbOutStream> jsonb_writer;
    Arena pool;
    jsonb_writer.writeStartObject();
    serde->write_one_cell_to_jsonb(input_column, jsonb_writer, pool, row_num, 0, options);
    jsonb_writer.writeEndObject();

    auto jsonb_column = ColumnString::create();
    jsonb_column->insert_data(jsonb_writer.getOutput()->getBuffer(),
                              jsonb_writer.getOutput()->getSize());
    StringRef jsonb_data = jsonb_column->get_data_at(0);

    const JsonbDocument* pdoc = nullptr;
    auto st = JsonbDocument::checkAndCreateDocument(jsonb_data.data, jsonb_data.size, &pdoc);
    ASSERT_TRUE(st.ok()) << st.to_string();

    auto output_column = data_type->create_column();
    const JsonbDocument& doc = *pdoc;
    for (auto it = doc->begin(); it != doc->end(); ++it) {
        CastParameters params;
        params.is_strict = false;
        ASSERT_TRUE(serde->read_one_cell_from_jsonb(*output_column, it->value(), params).ok());
    }
    EXPECT_EQ(data_type->to_string(input_column, row_num), data_type->to_string(*output_column, 0));
}

} // namespace

TEST(UnalignedSerDeTest, PbDecimalV2RoundTrip) {
    auto col = ColumnDecimal128V2::create(0, 9);
    auto& data = col->get_data();
    for (int i = 0; i < 100; ++i) {
        data.push_back(Decimal128V2(__int128(i) * 1000000000 + i));
    }
    DataTypePtr data_type(std::make_shared<DataTypeDecimal<Decimal128V2>>(27, 9));
    check_pb_round_trip(data_type, *col);
}

TEST(UnalignedSerDeTest, PbDecimal128IRoundTrip) {
    auto col = ColumnDecimal128V3::create(0, 2);
    auto& data = col->get_data();
    for (int i = 0; i < 100; ++i) {
        data.push_back(Decimal128V3(__int128(i) * 100 + i));
    }
    DataTypePtr data_type(std::make_shared<DataTypeDecimal<Decimal128V3>>(38, 2));
    check_pb_round_trip(data_type, *col);
}

TEST(UnalignedSerDeTest, PbDecimal256RoundTrip) {
    auto col = ColumnDecimal256::create(0, 0);
    auto& data = col->get_data();
    for (int i = 0; i < 100; ++i) {
        wide::Int256 val = wide::Int256(i) * wide::Int256(1000000000);
        data.push_back(Decimal256(val));
    }
    DataTypePtr data_type(std::make_shared<DataTypeDecimal<Decimal256>>(76, 0));
    check_pb_round_trip(data_type, *col);
}

TEST(UnalignedSerDeTest, PbLargeIntRoundTrip) {
    auto col = ColumnInt128::create();
    auto& data = col->get_data();
    for (int i = 0; i < 100; ++i) {
        data.push_back(__int128(i) * ((__int128)1 << 64) + i);
    }
    DataTypePtr data_type(std::make_shared<DataTypeInt128>());
    check_pb_round_trip(data_type, *col);
}

TEST(UnalignedSerDeTest, PbIPv6RoundTrip) {
    auto col = ColumnIPv6::create();
    auto& data = col->get_data();
    for (int i = 0; i < 100; ++i) {
        IPv6 val;
        memcpy(&val, &i, sizeof(i));
        data.push_back(val);
    }
    DataTypePtr data_type(std::make_shared<DataTypeIPv6>());
    check_pb_round_trip(data_type, *col);
}

TEST(UnalignedSerDeTest, JsonbDecimalV2RoundTrip) {
    auto col = ColumnDecimal128V2::create(0, 9);
    Decimal128V2 val(__int128(12345) * 1000000000LL + 67890);
    col->insert_value(val);
    DataTypePtr data_type(std::make_shared<DataTypeDecimal<Decimal128V2>>(27, 9));
    check_jsonb_round_trip(data_type, *col, 0);
}

TEST(UnalignedSerDeTest, JsonbDecimal128IRoundTrip) {
    auto col = ColumnDecimal128V3::create(0, 2);
    Decimal128V3 val(12345);
    col->insert_value(val);
    DataTypePtr data_type(std::make_shared<DataTypeDecimal<Decimal128V3>>(38, 2));
    check_jsonb_round_trip(data_type, *col, 0);
}

TEST(UnalignedSerDeTest, JsonbDecimal256RoundTrip) {
    auto col = ColumnDecimal256::create(0, 0);
    wide::Int256 val = wide::Int256(12345);
    col->insert_value(Decimal256(val));
    DataTypePtr data_type(std::make_shared<DataTypeDecimal<Decimal256>>(76, 0));
    check_jsonb_round_trip(data_type, *col, 0);
}

} // namespace doris
