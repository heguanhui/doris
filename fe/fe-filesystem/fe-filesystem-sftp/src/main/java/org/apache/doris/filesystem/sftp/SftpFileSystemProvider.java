package org.apache.doris.filesystem.sftp;

import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.spi.FileSystemProvider;

import java.io.IOException;
import java.util.Map;

public class SftpFileSystemProvider implements FileSystemProvider {

    @Override
    public boolean supports(Map<String, String> properties) {
        String storageType = properties.get("_STORAGE_TYPE_");
        if ("SFTP".equalsIgnoreCase(storageType)) {
            return true;
        }
        String uri = properties.getOrDefault("uri", "").toLowerCase();
        return uri.startsWith("sftp://");
    }

    @Override
    public FileSystem create(Map<String, String> properties) throws IOException {
        return new SftpFileSystem(properties);
    }

    @Override
    public String name() {
        return "SFTP";
    }
}
