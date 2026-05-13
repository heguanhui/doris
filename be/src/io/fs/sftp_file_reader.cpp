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

#include "io/fs/sftp_file_reader.h"

#include <curl/curl.h>
#include <curl/easy.h>

#include <cstring>

#include "common/logging.h"

namespace doris::io {

Result<FileReaderSPtr> SftpFileReader::create(const std::string& path,
                                              const std::map<std::string, std::string>& properties,
                                              const FileReaderOptions& opts,
                                              RuntimeProfile* /*profile*/) {
    std::string uri;
    auto uri_iter = properties.find("uri");
    if (uri_iter != properties.end()) {
        uri = uri_iter->second;
    }

    std::string user;
    auto user_iter = properties.find("user");
    if (user_iter != properties.end()) {
        user = user_iter->second;
    }

    std::string password;
    auto password_iter = properties.find("password");
    if (password_iter != properties.end()) {
        password = password_iter->second;
    }

    std::string ssh_key;
    auto ssh_key_iter = properties.find("ssh_key");
    if (ssh_key_iter != properties.end()) {
        ssh_key = ssh_key_iter->second;
    }

    int64_t file_size = opts.file_size;
    if (file_size < 0) {
        CURL* curl = curl_easy_init();
        if (!curl) {
            return Status::InternalError("Failed to initialize curl for SFTP file size");
        }

        std::string url = uri + path;
        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_PROTOCOLS, CURLPROTO_SFTP);
        curl_easy_setopt(curl, CURLOPT_USERNAME, user.c_str());
        curl_easy_setopt(curl, CURLOPT_PASSWORD, password.c_str());
        if (!ssh_key.empty()) {
            curl_easy_setopt(curl, CURLOPT_SSH_AUTH_TYPES, CURLSSH_AUTH_PUBLICKEY);
            curl_easy_setopt(curl, CURLOPT_SSH_PRIVATE_KEYFILE, ssh_key.c_str());
        } else {
            curl_easy_setopt(curl, CURLOPT_SSH_AUTH_TYPES, CURLSSH_AUTH_PASSWORD);
        }
        curl_easy_setopt(curl, CURLOPT_NOBODY, 1L);
        curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 10L);

        CURLcode res = curl_easy_perform(curl);
        if (res != CURLE_OK) {
            curl_easy_cleanup(curl);
            return Status::InternalError("SFTP file size check failed: {}",
                                         curl_easy_strerror(res));
        }

        double filesize = 0;
        curl_easy_getinfo(curl, CURLINFO_CONTENT_LENGTH_DOWNLOAD_T, &filesize);
        curl_easy_cleanup(curl);

        file_size = static_cast<int64_t>(filesize);
    }

    auto reader = std::make_shared<SftpFileReader>(path, file_size, opts.mtime, properties);
    RETURN_IF_ERROR_RESULT(reader->open());

    return reader;
}

SftpFileReader::SftpFileReader(const std::string& path, int64_t file_size, int64_t mtime,
                               const std::map<std::string, std::string>& properties)
        : _path(path),
          _file_size(file_size < 0 ? static_cast<size_t>(-1) : static_cast<size_t>(file_size)),
          _mtime(mtime),
          _properties(properties) {
    auto uri_iter = _properties.find("uri");
    if (uri_iter != _properties.end()) {
        _uri = uri_iter->second;
    }

    auto user_iter = _properties.find("user");
    if (user_iter != _properties.end()) {
        _user = user_iter->second;
    }

    auto password_iter = _properties.find("password");
    if (password_iter != _properties.end()) {
        _password = password_iter->second;
    }

    auto ssh_key_iter = _properties.find("ssh_key");
    if (ssh_key_iter != _properties.end()) {
        _ssh_key = ssh_key_iter->second;
    }
}

SftpFileReader::~SftpFileReader() {
    static_cast<void>(close());
}

Status SftpFileReader::open() {
    if (_closed.load(std::memory_order_acquire)) {
        return Status::InternalError("SFTP file reader is closed");
    }
    return Status::OK();
}

Status SftpFileReader::_load_file() {
    if (_data_loaded) return Status::OK();

    CURL* curl = curl_easy_init();
    if (!curl) {
        return Status::InternalError("Failed to initialize curl for SFTP read");
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
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 10L);

    _data.clear();
    auto cb = [](char* ptr, size_t size, size_t nmemb, void* userdata) -> size_t {
        auto* data = static_cast<std::string*>(userdata);
        size_t total = size * nmemb;
        data->append(ptr, total);
        return total;
    };
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, cb);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &_data);

    CURLcode res = curl_easy_perform(curl);
    curl_easy_cleanup(curl);

    if (res != CURLE_OK) {
        return Status::InternalError("SFTP read failed: {}", curl_easy_strerror(res));
    }

    _data_loaded = true;
    if (_file_size == static_cast<size_t>(-1)) {
        _file_size = _data.size();
    }
    return Status::OK();
}

Status SftpFileReader::read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                                    const IOContext* /*io_ctx*/) {
    if (_closed.load(std::memory_order_acquire)) {
        return Status::InternalError("SFTP file reader is closed");
    }

    RETURN_IF_ERROR(_load_file());

    if (offset >= _data.size()) {
        *bytes_read = 0;
        return Status::OK();
    }

    size_t available = _data.size() - offset;
    size_t copy_len = std::min(available, result.size);
    std::memcpy(result.data, _data.data() + offset, copy_len);
    *bytes_read = copy_len;

    return Status::OK();
}

Status SftpFileReader::close() {
    if (_closed.exchange(true)) {
        return Status::OK();
    }

    _data.clear();
    _data.shrink_to_fit();
    _data_loaded = false;

    return Status::OK();
}

} // namespace doris::io
