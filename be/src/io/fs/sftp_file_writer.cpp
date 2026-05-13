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

#include "io/fs/sftp_file_writer.h"

#include <curl/curl.h>
#include <curl/easy.h>

#include <cstring>
#include <utility>

#include "common/logging.h"

namespace doris::io {

SftpFileWriter::SftpFileWriter(Path path, const std::map<std::string, std::string>& properties)
        : _path(std::move(path)), _properties(properties) {
    auto it = properties.find("uri");
    if (it != properties.end()) {
        _uri = it->second;
    }
    it = properties.find("user");
    if (it != properties.end()) {
        _user = it->second;
    }
    it = properties.find("password");
    if (it != properties.end()) {
        _password = it->second;
    }
    it = properties.find("ssh_key");
    if (it != properties.end()) {
        _ssh_key = it->second;
    }
}

SftpFileWriter::~SftpFileWriter() = default;

Result<std::unique_ptr<SftpFileWriter>> SftpFileWriter::create(
        const std::string& path, const std::map<std::string, std::string>& properties,
        const FileWriterOptions* opts) {
    auto writer = std::make_unique<SftpFileWriter>(Path(path), properties);
    return writer;
}

Status SftpFileWriter::appendv(const Slice* data, size_t data_cnt) {
    if (_state != State::OPENED) [[unlikely]] {
        return Status::InternalError("append to closed file: {}", _path.native());
    }
    for (size_t i = 0; i < data_cnt; i++) {
        _buffer.append(data[i].data, data[i].size);
        _bytes_appended += data[i].size;
    }
    return Status::OK();
}

Status SftpFileWriter::close(bool non_block) {
    if (_state == State::CLOSED) {
        return Status::InternalError("SftpFileWriter already closed, file path {}",
                                     _path.native());
    }
    RETURN_IF_ERROR(_upload());
    _state = State::CLOSED;
    return Status::OK();
}

Status SftpFileWriter::_upload() {
    CURL* curl = curl_easy_init();
    if (!curl) {
        return Status::InternalError("Failed to initialize curl for SFTP upload");
    }

    std::string url = _uri + _path.native();
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_PROTOCOLS, CURLPROTO_SFTP);
    curl_easy_setopt(curl, CURLOPT_USERNAME, _user.c_str());
    curl_easy_setopt(curl, CURLOPT_PASSWORD, _password.c_str());
    if (!_ssh_key.empty()) {
        curl_easy_setopt(curl, CURLOPT_SSH_AUTH_TYPES, CURLSSH_AUTH_PUBLICKEY);
        curl_easy_setopt(curl, CURLOPT_SSH_PRIVATE_KEYFILE, _ssh_key.c_str());
    } else {
        curl_easy_setopt(curl, CURLOPT_SSH_AUTH_TYPES, CURLSSH_AUTH_PASSWORD);
    }
    curl_easy_setopt(curl, CURLOPT_UPLOAD, 1L);
    curl_easy_setopt(curl, CURLOPT_INFILESIZE_LARGE,
                     static_cast<curl_off_t>(_buffer.size()));
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 10L);

    size_t offset = 0;
    auto cb = [](char* ptr, size_t size, size_t nmemb, void* userdata) -> size_t {
        auto* ctx = static_cast<std::pair<std::string*, size_t>*>(userdata);
        size_t remaining = ctx->first->size() - *ctx->second;
        size_t to_copy = std::min(remaining, size * nmemb);
        if (to_copy == 0) return 0;
        std::memcpy(ptr, ctx->first->data() + *ctx->second, to_copy);
        *ctx->second += to_copy;
        return to_copy;
    };
    auto ctx = std::make_pair(&_buffer, &offset);
    curl_easy_setopt(curl, CURLOPT_READFUNCTION, cb);
    curl_easy_setopt(curl, CURLOPT_READDATA, &ctx);

    CURLcode res = curl_easy_perform(curl);
    curl_easy_cleanup(curl);

    if (res != CURLE_OK) {
        return Status::InternalError("SFTP upload failed: {}", curl_easy_strerror(res));
    }
    return Status::OK();
}

} // namespace doris::io
