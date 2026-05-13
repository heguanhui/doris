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

#include <atomic>
#include <map>
#include <memory>
#include <string>

#include "common/status.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_system.h"
#include "runtime/runtime_profile.h"
#include "util/slice.h"

namespace doris::io {

class FtpFileReader final : public FileReader {
public:
    static Result<FileReaderSPtr> create(const std::string& path,
                                         const std::map<std::string, std::string>& properties,
                                         const FileReaderOptions& opts, RuntimeProfile* profile);
    explicit FtpFileReader(const std::string& path, int64_t file_size, int64_t mtime,
                           const std::map<std::string, std::string>& properties);
    ~FtpFileReader() override;

    Status open();
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const IOContext* io_ctx = nullptr) override;
    Status close() override;
    const Path& path() const override { return _path; }
    bool closed() const override { return _closed.load(std::memory_order_acquire); }
    size_t size() const override { return _file_size; }
    int64_t mtime() const override { return _mtime; }

private:
    std::string _uri;
    std::string _user;
    std::string _password;
    Path _path;
    size_t _file_size;
    int64_t _mtime;
    std::atomic<bool> _closed = false;
    std::map<std::string, std::string> _properties;
    std::string _data;
    bool _data_loaded = false;

    Status _load_file();
};

} // namespace doris::io
