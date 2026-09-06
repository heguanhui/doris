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

#include "format/table/info_schema_table_reader.h"

#include <gen_cpp/Data_types.h>

#include "common/object_pool.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/types.h"
#include "core/value/vdatetime_value.h"
#include "core/data_type/data_type_factory.hpp"
#include "exprs/function/cast/cast_to_date_or_datetime_impl.hpp"
#include "information_schema/schema_helper.h"
#include "information_schema/schema_scanner.h"
#include "information_schema/schema_tablets_scanner.h"
#include "runtime/cluster_info.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "runtime/runtime_profile.h"

namespace doris {

InfoSchemaTableReader::InfoSchemaTableReader(const std::vector<SlotDescriptor*>& file_slot_descs,
                                             RuntimeState* state,
                                             const TFileRangeDesc& range)
        : _file_slot_descs(file_slot_descs),
          _state(state),
          _time_zone(state->timezone()),
          _query_id(state->query_id()) {
    if (range.__isset.table_format_params && range.table_format_params.__isset.info_schema_params) {
        _info_schema_desc = range.table_format_params.info_schema_params;
        _has_info_schema_desc = true;
        // Detect BE-local metadata type.
        if (_info_schema_desc.__isset.source_info &&
            _info_schema_desc.source_info.__isset.source_type &&
            _info_schema_desc.source_info.source_type == TInfoSchemaSourceType::BE_LOCAL) {
            _is_be_local = true;
        }
    }
    if (_state->query_options().__isset.batch_size && _state->query_options().batch_size > 0) {
        _batch_size = _state->query_options().batch_size;
    }
}

InfoSchemaTableReader::~InfoSchemaTableReader() = default;

Status InfoSchemaTableReader::_do_init_reader(ReaderInitContext* /*ctx*/) {
    // NVI: core init. Validate that the scan range carries info_schema_params.
    if (!_has_info_schema_desc) {
        return Status::InternalError(
                "info_schema_params is not set in TFileRangeDesc for information_schema reader");
    }
    // BE-local: lazily initialize the scanner in _fetch_next_batch_be_local().
    return Status::OK();
}

Status InfoSchemaTableReader::_get_columns_impl(
        std::unordered_map<std::string, DataTypePtr>* name_to_type) {
    // All projected columns are materialized by FE; nothing is missing, so
    // FileScanner::_fill_missing_columns() becomes a no-op.
    for (const auto* slot : _file_slot_descs) {
        name_to_type->emplace(slot->col_name(), slot->type());
    }
    return Status::OK();
}

Status InfoSchemaTableReader::_insert_cell(const TCell& cell, int col_index, Block* block,
                                           const DataTypePtr& data_type) {
    auto column_guard = block->mutate_column_scoped(col_index);
    auto* nullable_column = assert_cast<ColumnNullable*>(column_guard.mutable_column().get());
    if (cell.__isset.isNull && cell.isNull) {
        // Explicit NULL cell: push 1 into the null map without touching the nested column.
        nullable_column->get_null_map_data().push_back(1);
        return Status::OK();
    }
    IColumn* col_ptr = &nullable_column->get_nested_column();
    switch (data_type->get_primitive_type()) {
    case TYPE_BIGINT:
        assert_cast<ColumnInt64*>(col_ptr)->insert_value(cell.longVal);
        break;
    case TYPE_INT:
        assert_cast<ColumnInt32*>(col_ptr)->insert_value(cell.intVal);
        break;
    case TYPE_SMALLINT:
        assert_cast<ColumnInt16*>(col_ptr)->insert_value(static_cast<int16_t>(cell.intVal));
        break;
    case TYPE_TINYINT:
        assert_cast<ColumnInt8*>(col_ptr)->insert_value(static_cast<int8_t>(cell.intVal));
        break;
    case TYPE_FLOAT:
        assert_cast<ColumnFloat32*>(col_ptr)->insert_value(cell.doubleVal);
        break;
    case TYPE_DOUBLE:
        assert_cast<ColumnFloat64*>(col_ptr)->insert_value(cell.doubleVal);
        break;
    case TYPE_BOOLEAN:
        assert_cast<ColumnUInt8*>(col_ptr)->insert_value(cell.boolVal);
        break;
    case TYPE_STRING:
    case TYPE_VARCHAR:
    case TYPE_CHAR:
        assert_cast<ColumnString*>(col_ptr)->insert_data(cell.stringVal.data(),
                                                         cell.stringVal.size());
        break;
    case TYPE_DATETIME:
    case TYPE_DATE: {
        VecDateTimeValue src;
        CastParameters params;
        if (data_type->get_primitive_type() == TYPE_DATE) {
            CastToDateOrDatetime::from_string_non_strict_mode<DatelikeTargetType::DATE>(
                    {cell.stringVal.data(), cell.stringVal.size()}, src, nullptr, params);
        } else {
            CastToDateOrDatetime::from_string_non_strict_mode<DatelikeTargetType::DATE_TIME>(
                    {cell.stringVal.data(), cell.stringVal.size()}, src, nullptr, params);
        }
        if (data_type->get_primitive_type() == TYPE_DATE) {
            assert_cast<ColumnDate*>(col_ptr)->insert_data(reinterpret_cast<char*>(&src), 0);
        } else {
            assert_cast<ColumnDateTime*>(col_ptr)->insert_data(reinterpret_cast<char*>(&src), 0);
        }
        break;
    }
    default: {
        std::stringstream ss;
        ss << "unsupported column type:" << data_type->get_name();
        return Status::InternalError(ss.str());
    }
    }
    nullable_column->push_false_to_nullmap(1);
    return Status::OK();
}

Status InfoSchemaTableReader::_fetch_next_batch() {
    if (_is_be_local) {
        return _fetch_next_batch_be_local();
    }
    TFetchSchemaTableDataRequest request = _build_request();
    // Phase 1 simplification: always target the master FE, same as the existing
    // SchemaPartitionsScanner path. The multi-FE routing based on
    // TInfoSchemaSourceInfo.fe_addr_list is reserved for a follow-up.
    TNetworkAddress master_addr = ExecEnv::GetInstance()->cluster_info()->master_fe_addr;
    TFetchSchemaTableDataResult result;
    RETURN_IF_ERROR(SchemaHelper::fetch_schema_table_data(master_addr.hostname, master_addr.port,
                                                          request, &result, 5000));
    Status status = Status::create(result.status);
    if (!status.ok()) {
        LOG(WARNING) << "fetch information_schema data from FE failed, errmsg=" << status;
        return status;
    }
    _batch_data = std::move(result.data_batch);
    _batch_offset = 0;
    _total_fetched_rows += _batch_data.size();
    _has_more = result.__isset.has_more && result.has_more;
    return Status::OK();
}

Status InfoSchemaTableReader::_fetch_next_batch_be_local() {
    // BE-local metadata: fetch the next Block from the local SchemaScanner.
    if (!_be_local_scanner_initialized) {
        _be_local_scanner = SchemaScanner::create(TSchemaTableType::SCH_BACKEND_TABLETS);
        if (!_be_local_scanner) {
            return Status::InternalError("failed to create SchemaTabletsScanner");
        }
        SchemaScannerParam param;
        param.common_param = std::make_shared<SchemaScannerCommonParam>();
        // thread_id is not critical for BE-local scanners; set to 0.
        param.common_param->thread_id = 0;
        ObjectPool* pool = _state->obj_pool();
        RETURN_IF_ERROR(_be_local_scanner->init(_state, &param, pool));
        RETURN_IF_ERROR(_be_local_scanner->start(_state));
        _be_local_scanner_initialized = true;
        // Manually initialize the internal data block and fetch the first batch
        // synchronously, since we are not running inside a pipeline execution context.
        _be_local_scanner_data_block = Block::create_unique();
        const auto& columns = _be_local_scanner->get_column_desc();
        for (const auto& col : columns) {
            auto data_type = DataTypeFactory::instance().create_data_type(col.type, true);
            _be_local_scanner_data_block->insert(
                    ColumnWithTypeAndName(data_type->create_column(), data_type, col.name));
        }
        _be_local_scanner_eos = false;
        RETURN_IF_ERROR(_be_local_scanner->get_next_block_internal(
                _be_local_scanner_data_block.get(), &_be_local_scanner_eos));
    } else {
        // Fetch subsequent batches.
        _be_local_scanner_data_block->clear_column_data();
        _be_local_scanner_eos = false;
        RETURN_IF_ERROR(_be_local_scanner->get_next_block_internal(
                _be_local_scanner_data_block.get(), &_be_local_scanner_eos));
    }

    // Copy data from the scanner's internal block to our output block.
    _be_local_block.clear();
    if (_be_local_scanner_data_block->rows() > 0) {
        size_t src_col_count = _be_local_scanner_data_block->columns();
        // Create output columns from scanner block.
        for (size_t col = 0; col < src_col_count; ++col) {
            auto& src_col = _be_local_scanner_data_block->get_by_position(col);
            auto nullable_col = src_col.type->create_column();
            const auto* src_nullable = assert_cast<const ColumnNullable*>(src_col.column.get());
            nullable_col->insert_range_from(*src_nullable, 0, src_nullable->size());
            _be_local_block.insert({std::move(nullable_col), src_col.type, src_col.name});
        }
    }
    _be_local_block_offset = 0;
    _be_local_block_empty = (_be_local_block.rows() == 0);
    _has_more = !_be_local_scanner_eos;
    _total_fetched_rows += _be_local_block.rows();
    return Status::OK();
}

TFetchSchemaTableDataRequest InfoSchemaTableReader::_build_request() const {
    TFetchSchemaTableDataRequest request;
    request.__set_info_schema_params(_info_schema_desc);
    request.__set_offset(_total_fetched_rows);
    request.__set_batch_size(_batch_size);
    request.__set_time_zone(_time_zone);
    // query_id is the QueryScopedMetadataCache key component (design §2.2 Optimization 1).
    request.__set_query_id(_query_id);
    // Column projection: names in slot order, aligned with the TRow column_value order
    // returned by FE (both follow TInfoSchemaFileDesc.output_mode).
    std::vector<std::string> columns_name;
    columns_name.reserve(_file_slot_descs.size());
    for (const auto* slot : _file_slot_descs) {
        columns_name.emplace_back(slot->col_name());
    }
    request.__set_columns_name(std::move(columns_name));
    return request;
}

Status InfoSchemaTableReader::_do_get_next_block(Block* block, size_t* read_rows, bool* eof) {
    if (block == nullptr || eof == nullptr) {
        return Status::InternalError("input pointer is nullptr.");
    }
    *read_rows = 0;
    *eof = false;
    if (_eof) {
        *eof = true;
        return Status::OK();
    }

    if (_is_be_local) {
        // BE-local path: directly copy rows from _be_local_block.
        while (*read_rows < static_cast<size_t>(_batch_size)) {
            if (_be_local_block_empty || _be_local_block_offset >= _be_local_block.rows()) {
                if (!_has_more) {
                    _eof = true;
                    *eof = true;
                    break;
                }
                RETURN_IF_ERROR(_fetch_next_batch_be_local());
                if (_be_local_block_empty) {
                    if (!_has_more) {
                        _eof = true;
                        *eof = true;
                    }
                    break;
                }
                continue;
            }
            // Copy one row from _be_local_block to the output block.
            size_t src_col_count = _be_local_block.columns();
            size_t dst_col_count = block->columns();
            size_t copy_cols = std::min(src_col_count, dst_col_count);
            for (size_t col = 0; col < copy_cols; ++col) {
                auto col_guard = block->mutate_column_scoped(col);
                auto* nullable_col = assert_cast<ColumnNullable*>(col_guard.mutable_column().get());
                const auto* src_nullable = assert_cast<const ColumnNullable*>(
                        _be_local_block.get_by_position(col).column.get());
                // Copy null map.
                nullable_col->get_null_map_data().push_back(
                        src_nullable->get_null_map_data()[_be_local_block_offset]);
                // Copy data.
                nullable_col->get_nested_column().insert_from(
                        src_nullable->get_nested_column(), _be_local_block_offset);
            }
            ++_be_local_block_offset;
            ++(*read_rows);
        }
        if (*read_rows == 0 && !_eof) {
            _eof = true;
            *eof = true;
        }
        return Status::OK();
    }

    // FE-metadata path: read from _batch_data (vector<TRow>).
    while (*read_rows < static_cast<size_t>(_batch_size)) {
        if (_batch_offset >= _batch_data.size()) {
            if (!_has_more) {
                _eof = true;
                *eof = true;
                break;
            }
            RETURN_IF_ERROR(_fetch_next_batch());
            if (_batch_data.empty()) {
                if (!_has_more) {
                    _eof = true;
                    *eof = true;
                }
                break;
            }
            continue;
        }
        const TRow& row = _batch_data[_batch_offset];
        if (row.column_value.size() != _file_slot_descs.size()) {
            return Status::InternalError(
                    "information_schema row schema mismatch: FE returned {} columns, BE expects "
                    "{} columns",
                    row.column_value.size(), _file_slot_descs.size());
        }
        for (size_t j = 0; j < _file_slot_descs.size(); ++j) {
            RETURN_IF_ERROR(
                    _insert_cell(row.column_value[j], static_cast<int>(j), block,
                                 _file_slot_descs[j]->type()));
        }
        ++_batch_offset;
        ++(*read_rows);
    }
    if (*read_rows == 0 && !_eof) {
        _eof = true;
        *eof = true;
    }
    return Status::OK();
}

} // namespace doris

