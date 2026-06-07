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

#include <cmath>
#include <limits>

#include "core/data_type/data_type_decimal.h"
#include "core/wide_integer_impl.h"
#include "exprs/function/cast/cast_to_decimal.h"

namespace doris {

TEST(CastToDecimalFromFloatTest, Decimal256OverflowFromDouble) {
    constexpr UInt32 precision = 76;
    constexpr UInt32 scale = 0;
    auto scale_multiplier = DataTypeDecimal<TYPE_DECIMAL256>::get_scale_multiplier(scale);
    auto max_result = DataTypeDecimal<TYPE_DECIMAL256>::get_max_digits_number(precision);
    auto min_result = -max_result;

    {
        Decimal256 result;
        CastParameters params;
        double val = 1e70;
        EXPECT_TRUE(CastToDecimal::from_float(val, result, precision, scale, params));
    }

    {
        Decimal256 result;
        CastParameters params;
        double val = -1e70;
        EXPECT_TRUE(CastToDecimal::from_float(val, result, precision, scale, params));
    }

    {
        Decimal256 result;
        CastParameters params;
        double val = 1e76;
        EXPECT_FALSE(CastToDecimal::from_float(val, result, precision, scale, params));
    }

    {
        Decimal256 result;
        CastParameters params;
        double val = -1e76;
        EXPECT_FALSE(CastToDecimal::from_float(val, result, precision, scale, params));
    }

    {
        Decimal256 result;
        CastParameters params;
        double val = std::numeric_limits<double>::infinity();
        EXPECT_FALSE(CastToDecimal::from_float(val, result, precision, scale, params));
    }

    {
        Decimal256 result;
        CastParameters params;
        double val = -std::numeric_limits<double>::infinity();
        EXPECT_FALSE(CastToDecimal::from_float(val, result, precision, scale, params));
    }

    {
        Decimal256 result;
        CastParameters params;
        double val = std::numeric_limits<double>::quiet_NaN();
        EXPECT_FALSE(CastToDecimal::from_float(val, result, precision, scale, params));
    }
}

TEST(CastToDecimalFromFloatTest, Decimal256OverflowFromFloat) {
    constexpr UInt32 precision = 76;
    constexpr UInt32 scale = 0;

    {
        Decimal256 result;
        CastParameters params;
        float val = 1e30f;
        EXPECT_TRUE(CastToDecimal::from_float(val, result, precision, scale, params));
    }

    {
        Decimal256 result;
        CastParameters params;
        float val = std::numeric_limits<float>::infinity();
        EXPECT_FALSE(CastToDecimal::from_float(val, result, precision, scale, params));
    }

    {
        Decimal256 result;
        CastParameters params;
        float val = -std::numeric_limits<float>::infinity();
        EXPECT_FALSE(CastToDecimal::from_float(val, result, precision, scale, params));
    }

    {
        Decimal256 result;
        CastParameters params;
        float val = std::numeric_limits<float>::quiet_NaN();
        EXPECT_FALSE(CastToDecimal::from_float(val, result, precision, scale, params));
    }
}

TEST(CastToDecimalFromFloatTest, Decimal256BoundaryFromDoubleWithScale) {
    constexpr UInt32 precision = 76;
    constexpr UInt32 scale = 38;

    {
        Decimal256 result;
        CastParameters params;
        double val = 1e37;
        EXPECT_TRUE(CastToDecimal::from_float(val, result, precision, scale, params));
    }

    {
        Decimal256 result;
        CastParameters params;
        double val = 1e38;
        EXPECT_FALSE(CastToDecimal::from_float(val, result, precision, scale, params));
    }

    {
        Decimal256 result;
        CastParameters params;
        double val = -1e38;
        EXPECT_FALSE(CastToDecimal::from_float(val, result, precision, scale, params));
    }
}

TEST(CastToDecimalFromFloatTest, Decimal256FromDoubleIntermediateTypePrecision) {
    using DoubleType = std::conditional_t<true, FromDoubleIntermediateType, double>;
    wide::Int256 max_int256 = std::numeric_limits<wide::Int256>::max();
    wide::Int256 min_int256 = std::numeric_limits<wide::Int256>::min();

    DoubleType converted_max = static_cast<DoubleType>(max_int256);
    DoubleType converted_min = static_cast<DoubleType>(min_int256);

    EXPECT_GT(static_cast<DoubleType>(max_int256), DoubleType(0));
    EXPECT_LT(static_cast<DoubleType>(min_int256), DoubleType(0));

    wide::Int256 back_from_max = static_cast<wide::Int256>(converted_max);
    EXPECT_GT(back_from_max, wide::Int256(0));
}

TEST(CastToDecimalFromFloatTest, Decimal128V3OverflowFromDouble) {
    constexpr UInt32 precision = 38;
    constexpr UInt32 scale = 0;

    {
        Decimal128V3 result;
        CastParameters params;
        double val = 1e37;
        EXPECT_TRUE(CastToDecimal::from_float(val, result, precision, scale, params));
    }

    {
        Decimal128V3 result;
        CastParameters params;
        double val = 1e38;
        EXPECT_FALSE(CastToDecimal::from_float(val, result, precision, scale, params));
    }

    {
        Decimal128V3 result;
        CastParameters params;
        double val = std::numeric_limits<double>::infinity();
        EXPECT_FALSE(CastToDecimal::from_float(val, result, precision, scale, params));
    }
}

} // namespace doris
