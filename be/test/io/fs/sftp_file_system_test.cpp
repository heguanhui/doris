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

#include "io/fs/sftp_file_system.h"

#include <gtest/gtest.h>

#include <map>
#include <string>

#include "common/status.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_writer.h"
#include "io/fs/sftp_file_reader.h"
#include "io/fs/sftp_file_writer.h"

namespace doris::io {

class SftpFileSystemTest : public testing::Test {
public:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(SftpFileSystemTest, TestCreateWithValidProperties) {
    std::map<std::string, std::string> properties;
    properties["uri"] = "sftp://sftp.example.com/";
    properties["user"] = "testuser";
    properties["password"] = "testpass";

    auto result = SftpFileSystem::create(properties, "sftp_test");
    ASSERT_TRUE(result.ok()) << result.status();
    auto fs = result.value();
    ASSERT_NE(fs, nullptr);
}

TEST_F(SftpFileSystemTest, TestCreateWithoutUri) {
    std::map<std::string, std::string> properties;
    properties["user"] = "testuser";
    properties["password"] = "testpass";

    auto result = SftpFileSystem::create(properties, "sftp_test");
    ASSERT_FALSE(result.ok());
    EXPECT_TRUE(result.status().to_string().find("uri") != std::string::npos);
}

TEST_F(SftpFileSystemTest, TestCreateWithInvalidUriScheme) {
    std::map<std::string, std::string> properties;
    properties["uri"] = "http://example.com/";
    properties["user"] = "testuser";
    properties["password"] = "testpass";

    auto result = SftpFileSystem::create(properties, "sftp_test");
    ASSERT_FALSE(result.ok());
    EXPECT_TRUE(result.status().to_string().find("sftp://") != std::string::npos);
}

TEST_F(SftpFileSystemTest, TestCreateWithFtpUri) {
    std::map<std::string, std::string> properties;
    properties["uri"] = "ftp://ftp.example.com/";
    properties["user"] = "testuser";
    properties["password"] = "testpass";

    auto result = SftpFileSystem::create(properties, "sftp_test");
    ASSERT_FALSE(result.ok());
}

TEST_F(SftpFileSystemTest, TestCreateWithSshKey) {
    std::map<std::string, std::string> properties;
    properties["uri"] = "sftp://sftp.example.com/";
    properties["user"] = "testuser";
    properties["ssh_key"] = "/home/user/.ssh/id_rsa";

    auto result = SftpFileSystem::create(properties, "sftp_test");
    ASSERT_TRUE(result.ok()) << result.status();
}

TEST_F(SftpFileSystemTest, TestCreateWithPasswordAuth) {
    std::map<std::string, std::string> properties;
    properties["uri"] = "sftp://sftp.example.com/";
    properties["user"] = "testuser";
    properties["password"] = "testpass";

    auto result = SftpFileSystem::create(properties, "sftp_test");
    ASSERT_TRUE(result.ok()) << result.status();
}

TEST_F(SftpFileSystemTest, TestCreateWithPort) {
    std::map<std::string, std::string> properties;
    properties["uri"] = "sftp://sftp.example.com:2222/";
    properties["user"] = "testuser";
    properties["password"] = "testpass";

    auto result = SftpFileSystem::create(properties, "sftp_test");
    ASSERT_TRUE(result.ok()) << result.status();
}

TEST_F(SftpFileSystemTest, TestNotSupportedOperations) {
    std::map<std::string, std::string> properties;
    properties["uri"] = "sftp://sftp.example.com/";
    properties["user"] = "testuser";
    properties["password"] = "testpass";

    auto result = SftpFileSystem::create(properties, "sftp_test");
    ASSERT_TRUE(result.ok());
    auto fs = result.value();

    Status st;
    st = fs->create_directory("/test_dir");
    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(st.to_string().find("not supported") != std::string::npos);

    st = fs->delete_file("/test_file");
    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(st.to_string().find("not supported") != std::string::npos);

    st = fs->delete_directory("/test_dir");
    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(st.to_string().find("not supported") != std::string::npos);

    std::vector<FileInfo> files;
    bool exists = false;
    st = fs->list("/test_dir", false, &files, &exists);
    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(st.to_string().find("not supported") != std::string::npos);

    st = fs->rename("/old", "/new");
    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(st.to_string().find("not supported") != std::string::npos);

    st = fs->download("/remote", "/local");
    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(st.to_string().find("not supported") != std::string::npos);

    st = fs->upload("/local", "/remote");
    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(st.to_string().find("not supported") != std::string::npos);
}

TEST_F(SftpFileSystemTest, TestFileSystemType) {
    std::map<std::string, std::string> properties;
    properties["uri"] = "sftp://sftp.example.com/";

    auto result = SftpFileSystem::create(properties, "sftp_test");
    ASSERT_TRUE(result.ok());
    auto fs = result.value();
    EXPECT_EQ(fs->type(), FileSystemType::SFTP);
}

class SftpFileReaderTest : public testing::Test {
public:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(SftpFileReaderTest, TestCreateWithKnownFileSize) {
    std::map<std::string, std::string> properties;
    properties["uri"] = "sftp://sftp.example.com/";
    properties["user"] = "testuser";
    properties["password"] = "testpass";

    FileReaderOptions opts;
    opts.file_size = 2048;
    opts.mtime = 0;

    auto result = SftpFileReader::create("/data/test.csv", properties, opts, nullptr);
    ASSERT_TRUE(result.ok()) << result.status();
    auto reader = result.value();
    EXPECT_EQ(reader->size(), 2048);
    EXPECT_FALSE(reader->closed());
}

TEST_F(SftpFileReaderTest, TestCreateWithSshKey) {
    std::map<std::string, std::string> properties;
    properties["uri"] = "sftp://sftp.example.com/";
    properties["user"] = "testuser";
    properties["ssh_key"] = "/home/user/.ssh/id_rsa";

    FileReaderOptions opts;
    opts.file_size = 1024;
    opts.mtime = 0;

    auto result = SftpFileReader::create("/data/test.csv", properties, opts, nullptr);
    ASSERT_TRUE(result.ok()) << result.status();
}

TEST_F(SftpFileReaderTest, TestCloseReader) {
    std::map<std::string, std::string> properties;
    properties["uri"] = "sftp://sftp.example.com/";

    FileReaderOptions opts;
    opts.file_size = 100;
    opts.mtime = 0;

    auto result = SftpFileReader::create("/data/test.csv", properties, opts, nullptr);
    ASSERT_TRUE(result.ok());
    auto reader = result.value();
    EXPECT_FALSE(reader->closed());

    auto st = reader->close();
    EXPECT_TRUE(st.ok());
    EXPECT_TRUE(reader->closed());

    st = reader->close();
    EXPECT_TRUE(st.ok());
}

TEST_F(SftpFileReaderTest, TestReadAfterClose) {
    std::map<std::string, std::string> properties;
    properties["uri"] = "sftp://sftp.example.com/";

    FileReaderOptions opts;
    opts.file_size = 100;
    opts.mtime = 0;

    auto result = SftpFileReader::create("/data/test.csv", properties, opts, nullptr);
    ASSERT_TRUE(result.ok());
    auto reader = result.value();
    reader->close();

    char buf[64];
    size_t bytes_read = 0;
    auto st = reader->read_at(0, Slice(buf, 64), &bytes_read);
    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(st.to_string().find("closed") != std::string::npos);
}

class SftpFileWriterTest : public testing::Test {
public:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(SftpFileWriterTest, TestCreateWriter) {
    std::map<std::string, std::string> properties;
    properties["uri"] = "sftp://sftp.example.com/";
    properties["user"] = "testuser";
    properties["password"] = "testpass";

    auto result = SftpFileWriter::create("/data/test.csv", properties);
    ASSERT_TRUE(result.ok()) << result.status();
    auto writer = std::move(*result);
    EXPECT_EQ(writer->bytes_appended(), 0);
    EXPECT_EQ(writer->state(), FileWriter::State::OPENED);
}

TEST_F(SftpFileWriterTest, TestCreateWriterWithSshKey) {
    std::map<std::string, std::string> properties;
    properties["uri"] = "sftp://sftp.example.com/";
    properties["user"] = "testuser";
    properties["ssh_key"] = "/home/user/.ssh/id_rsa";

    auto result = SftpFileWriter::create("/data/test.csv", properties);
    ASSERT_TRUE(result.ok()) << result.status();
}

TEST_F(SftpFileWriterTest, TestAppendData) {
    std::map<std::string, std::string> properties;
    properties["uri"] = "sftp://sftp.example.com/";

    auto result = SftpFileWriter::create("/data/test.csv", properties);
    ASSERT_TRUE(result.ok());
    auto writer = std::move(*result);

    Slice data("hello world");
    auto st = writer->appendv(&data, 1);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(writer->bytes_appended(), 11);
}

TEST_F(SftpFileWriterTest, TestAppendMultipleSlices) {
    std::map<std::string, std::string> properties;
    properties["uri"] = "sftp://sftp.example.com/";

    auto result = SftpFileWriter::create("/data/test.csv", properties);
    ASSERT_TRUE(result.ok());
    auto writer = std::move(*result);

    Slice slices[] = {Slice("hello"), Slice(" "), Slice("world")};
    auto st = writer->appendv(slices, 3);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(writer->bytes_appended(), 12);
}

TEST_F(SftpFileWriterTest, TestAppendAfterClose) {
    std::map<std::string, std::string> properties;
    properties["uri"] = "sftp://sftp.example.com/";

    auto result = SftpFileWriter::create("/data/test.csv", properties);
    ASSERT_TRUE(result.ok());
    auto writer = std::move(*result);

    Slice data("hello");
    writer->appendv(&data, 1);

    auto st = writer->close();
    EXPECT_EQ(writer->state(), FileWriter::State::CLOSED);

    st = writer->appendv(&data, 1);
    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(st.to_string().find("closed") != std::string::npos);
}

TEST_F(SftpFileWriterTest, TestDoubleClose) {
    std::map<std::string, std::string> properties;
    properties["uri"] = "sftp://sftp.example.com/";

    auto result = SftpFileWriter::create("/data/test.csv", properties);
    ASSERT_TRUE(result.ok());
    auto writer = std::move(*result);

    Slice data("hello");
    writer->appendv(&data, 1);

    auto st = writer->close();
    EXPECT_EQ(writer->state(), FileWriter::State::CLOSED);

    st = writer->close();
    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(st.to_string().find("already closed") != std::string::npos);
}

} // namespace doris::io
