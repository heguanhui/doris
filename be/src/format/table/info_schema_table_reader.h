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

#pragma once

#include <gen_cpp/Data_types.h>
#include <gen_cpp/FrontendService_types.h>
#include <gen_cpp/PlanNodes_types.h>

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "core/types.h"
#include "format/generic_reader.h"
#include "runtime/descriptors.h"

namespace doris {

class Block;
class RuntimeState;
class SchemaScanner;

// Reader for information_schema tables treated as a table format.
//
// Supports two data source types:
//   - FE_METADATA: rows are fetched from FE through the existing `fetchSchemaTableData` RPC.
//   - BE_LOCAL: each BE generates data locally using a SchemaScanner.
//
// NVI pattern (master GenericReader):
//   init_reader(ctx)  -> _open_file_reader -> on_before_init_reader -> _do_init_reader
//   get_next_block()  -> on_before_read_block -> _do_get_next_block -> on_after_read_block
// Subclasses implement _do_init_reader and _do_get_next_block.
class InfoSchemaTableReader final : public GenericReader {
public:
    InfoSchemaTableReader(const std::vector<SlotDescriptor*>& file_slot_descs,
                          RuntimeState* state,
                          const TFileRangeDesc& range);
    ~InfoSchemaTableReader();

    static std::unique_ptr<InfoSchemaTableReader> create_unique(
            const std::vector<SlotDescriptor*>& file_slot_descs, RuntimeState* state,
            const TFileRangeDesc& range) {
        return std::make_unique<InfoSchemaTableReader>(file_slot_descs, state, range);
    }

    Status close() override { return Status::OK(); }

protected:
    // GenericReader NVI hooks.
    Status _do_init_reader(ReaderInitContext* ctx) override;
    Status _do_get_next_block(Block* block, size_t* read_rows, bool* eof) override;
    Status _get_columns_impl(std::unordered_map<std::string, DataTypePtr>* name_to_type) override;

private:
    // Fetches the next page from FE. Uses the existing `fetchSchemaTableData` RPC through
    // SchemaHelper (the same helper used by the SchemaScanner path).
    Status _fetch_next_batch();

    // Fetches the next page from BE-local SchemaScanner.
    Status _fetch_next_batch_be_local();

    // Inserts one TCell into the target block column (copy of the SchemaScanner
    // convention; handles NULL cells explicitly).
    static Status _insert_cell(const TCell& cell, int col_index, Block* block,
                               const DataTypePtr& data_type);

    // Builds the RPC request: nests the scan-range-level TInfoSchemaFileDesc as-is
    // (single source of truth), plus paging control / runtime context / column
    // projection which are RPC-layer-only fields.
    TFetchSchemaTableDataRequest _build_request() const;

    const std::vector<SlotDescriptor*>& _file_slot_descs;
    RuntimeState* _state = nullptr;

    // Data source description (single source of truth, extracted from TFileRangeDesc).
    TInfoSchemaFileDesc _info_schema_desc;
    bool _has_info_schema_desc = false;
    bool _is_be_local = false;

    // Runtime context (from RuntimeState, not carried in the scan range layer).
    std::string _time_zone;
    TUniqueId _query_id;

    // Paging state.
    std::vector<TRow> _batch_data;
    size_t _batch_offset = 0;
    bool _has_more = true;
    int64_t _total_fetched_rows = 0;
    int32_t _batch_size = 4096;
    bool _eof = false;

    // BE-local scanner (owned, lazily initialized for BE_LOCAL metadata type).
    std::unique_ptr<SchemaScanner> _be_local_scanner;
    bool _be_local_scanner_initialized = false;
    // Scanner's internal data block (owned, used for synchronous access).
    std::unique_ptr<Block> _be_local_scanner_data_block;
    bool _be_local_scanner_eos = false;

    // BE-local paging state: directly from Block (avoids Block→TRow→Block conversion).
    Block _be_local_block;
    size_t _be_local_block_offset = 0;
    bool _be_local_block_empty = true;
};

} // namespace doris
