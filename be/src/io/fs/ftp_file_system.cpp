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

#include "io/fs/ftp_file_system.h"

#include <curl/curl.h>
#include <curl/easy.h>

#include "common/status.h"
#include "io/fs/err_utils.h"
#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "io/fs/ftp_file_reader.h"
#include "io/fs/ftp_file_writer.h"

namespace doris::io {

FtpFileSystem::FtpFileSystem(Path&& root_path, std::string id,
                             std::map<std::string, std::string> properties)
        : RemoteFileSystem(std::move(root_path), std::move(id), FileSystemType::FTP),
          _properties(std::move(properties)) {}

Status FtpFileSystem::_init() {
    auto uri_iter = _properties.find("uri");
    if (uri_iter == _properties.end()) {
        return Status::InvalidArgument("FTP uri is not specified");
    }
    _uri = uri_iter->second;
    if (_uri.find("ftp://") != 0) {
        return Status::InvalidArgument("FTP uri must start with ftp://, got: {}", _uri);
    }

    auto user_iter = _properties.find("user");
    _user = (user_iter != _properties.end()) ? user_iter->second : "";

    auto password_iter = _properties.find("password");
    _password = (password_iter != _properties.end()) ? password_iter->second : "";

    return Status::OK();
}

Result<std::shared_ptr<FtpFileSystem>> FtpFileSystem::create(
        const std::map<std::string, std::string>& properties, std::string id) {
    Path root_path = "";
    std::shared_ptr<FtpFileSystem> fs(
            new FtpFileSystem(std::move(root_path), std::move(id), properties));

    RETURN_IF_ERROR_RESULT(fs->_init());

    return fs;
}

Status FtpFileSystem::open_file_internal(const Path& file, FileReaderSPtr* reader,
                                         const FileReaderOptions& opts) {
    auto ftp_reader = FtpFileReader::create(file.native(), _properties, opts, nullptr);
    if (!ftp_reader.ok()) {
        return ftp_reader.status();
    }
    *reader = ftp_reader.value();
    return Status::OK();
}

Status FtpFileSystem::file_size_impl(const Path& file, int64_t* file_size) const {
    CURL* curl = curl_easy_init();
    if (!curl) {
        return Status::InternalError("Failed to initialize curl for FTP file size");
    }

    std::string url = _uri + file.native();
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_USERNAME, _user.c_str());
    curl_easy_setopt(curl, CURLOPT_PASSWORD, _password.c_str());
    curl_easy_setopt(curl, CURLOPT_NOBODY, 1L);
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 10L);
    curl_easy_setopt(curl, CURLOPT_FTP_RESPONSE_TIMEOUT, 30L);

    CURLcode res = curl_easy_perform(curl);
    if (res != CURLE_OK) {
        curl_easy_cleanup(curl);
        return Status::InternalError("FTP file size check failed: {}", curl_easy_strerror(res));
    }

    double filesize = 0;
    curl_easy_getinfo(curl, CURLINFO_CONTENT_LENGTH_DOWNLOAD_T, &filesize);
    curl_easy_cleanup(curl);

    *file_size = static_cast<int64_t>(filesize);
    return Status::OK();
}

Status FtpFileSystem::exists_impl(const Path& path, bool* res) const {
    int64_t file_size = 0;
    auto st = const_cast<FtpFileSystem*>(this)->file_size_impl(path, &file_size);
    if (st.ok()) {
        *res = true;
        return Status::OK();
    }
    *res = false;
    return Status::OK();
}

Status FtpFileSystem::create_file_impl(const Path& file, FileWriterPtr* writer,
                                       const FileWriterOptions* opts) {
    auto ftp_writer = FtpFileWriter::create(file.native(), _properties, opts);
    if (!ftp_writer.ok()) {
        return ftp_writer.status();
    }
    *writer = std::move(*ftp_writer);
    return Status::OK();
}

} // namespace doris::io
