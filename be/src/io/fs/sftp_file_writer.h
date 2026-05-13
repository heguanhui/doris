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

#include <map>
#include <memory>
#include <string>

#include "common/status.h"
#include "io/fs/file_writer.h"
#include "io/fs/path.h"
#include "util/slice.h"

namespace doris::io {

class SftpFileWriter final : public FileWriter {
public:
    static Result<std::unique_ptr<SftpFileWriter>> create(
            const std::string& path, const std::map<std::string, std::string>& properties,
            const FileWriterOptions* opts = nullptr);
    ~SftpFileWriter() override;

    Status close(bool non_block = false) override;
    Status appendv(const Slice* data, size_t data_cnt) override;
    const Path& path() const override { return _path; }
    size_t bytes_appended() const override { return _bytes_appended; }
    State state() const override { return _state; }

private:
    SftpFileWriter(Path path, const std::map<std::string, std::string>& properties);
    Status _upload();

    Path _path;
    std::string _uri;
    std::string _user;
    std::string _password;
    std::string _ssh_key;
    std::map<std::string, std::string> _properties;
    std::string _buffer;
    size_t _bytes_appended = 0;
    State _state = State::OPENED;
};

} // namespace doris::io
