package org.apache.doris.filesystem.ftp;

import org.apache.doris.filesystem.DorisInputFile;
import org.apache.doris.filesystem.DorisInputStream;
import org.apache.doris.filesystem.DorisOutputFile;
import org.apache.doris.filesystem.FileEntry;
import org.apache.doris.filesystem.FileIterator;
import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.Location;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FtpFileSystem implements FileSystem {

    private final Map<String, String> properties;
    private final String host;
    private int port = 21;
    private final String user;
    private final String password;

    public FtpFileSystem(Map<String, String> properties) {
        this.properties = properties;
        String uri = properties.getOrDefault("uri", "");
        URI parsed = URI.create(uri);
        this.host = parsed.getHost();
        if (parsed.getPort() > 0) {
            this.port = parsed.getPort();
        }
        this.user = properties.getOrDefault("user", "anonymous");
        this.password = properties.getOrDefault("password", "");
    }

    private FTPClient connect() throws IOException {
        FTPClient client = new FTPClient();
        client.connect(host, port);
        if (!client.login(user, password)) {
            client.disconnect();
            throw new IOException("FTP login failed for user: " + user);
        }
        client.enterLocalPassiveMode();
        client.setFileType(FTP.BINARY_FILE_TYPE);
        return client;
    }

    private String extractPath(Location location) {
        String uri = location.uri();
        URI parsed = URI.create(uri);
        String path = parsed.getPath();
        return path != null ? path : "/";
    }

    @Override
    public boolean exists(Location location) throws IOException {
        FTPClient client = connect();
        try {
            String path = extractPath(location);
            return client.listFiles(path).length > 0;
        } finally {
            client.logout();
            client.disconnect();
        }
    }

    @Override
    public void mkdirs(Location location) throws IOException {
        FTPClient client = connect();
        try {
            String path = extractPath(location);
            client.makeDirectory(path);
        } finally {
            client.logout();
            client.disconnect();
        }
    }

    @Override
    public void delete(Location location, boolean recursive) throws IOException {
        FTPClient client = connect();
        try {
            String path = extractPath(location);
            client.deleteFile(path);
        } finally {
            client.logout();
            client.disconnect();
        }
    }

    @Override
    public void rename(Location src, Location dst) throws IOException {
        FTPClient client = connect();
        try {
            client.rename(extractPath(src), extractPath(dst));
        } finally {
            client.logout();
            client.disconnect();
        }
    }

    @Override
    public FileIterator list(Location location) throws IOException {
        FTPClient client = connect();
        FTPFile[] files = client.listFiles(extractPath(location));
        List<FileEntry> entries = new ArrayList<>();
        String scheme = location.scheme();
        for (FTPFile f : files) {
            if (".".equals(f.getName()) || "..".equals(f.getName())) {
                continue;
            }
            String childUri = location.uri().replaceAll("/+$", "") + "/" + f.getName();
            entries.add(new FileEntry(
                Location.of(childUri),
                f.getSize(),
                f.isDirectory(),
                f.getTimestamp().getTimeInMillis(),
                null
            ));
        }
        client.logout();
        client.disconnect();
        return new FileIterator() {
            private int idx = 0;
            @Override
            public boolean hasNext() { return idx < entries.size(); }
            @Override
            public FileEntry next() { return entries.get(idx++); }
            @Override
            public void close() {}
        };
    }

    @Override
    public DorisInputFile newInputFile(Location location) throws IOException {
        return new DorisInputFile() {
            @Override
            public Location location() { return location; }

            @Override
            public long length() throws IOException {
                FTPClient client = connect();
                try {
                    String path = extractPath(location);
                    FTPFile[] files = client.listFiles(path);
                    if (files.length > 0) {
                        return files[0].getSize();
                    }
                    return 0;
                } finally {
                    client.logout();
                    client.disconnect();
                }
            }

            @Override
            public boolean exists() throws IOException {
                return FtpFileSystem.this.exists(location);
            }

            @Override
            public long lastModifiedTime() throws IOException {
                FTPClient client = connect();
                try {
                    FTPFile[] files = client.listFiles(extractPath(location));
                    if (files.length > 0) {
                        return files[0].getTimestamp().getTimeInMillis();
                    }
                    return 0;
                } finally {
                    client.logout();
                    client.disconnect();
                }
            }

            @Override
            public DorisInputStream newStream() throws IOException {
                FTPClient client = connect();
                String path = extractPath(location);
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                if (!client.retrieveFile(path, baos)) {
                    client.disconnect();
                    throw new IOException("Failed to retrieve file: " + path);
                }
                byte[] data = baos.toByteArray();
                client.logout();
                client.disconnect();
                return new DorisInputStream() {
                    private final ByteArrayInputStream bais = new ByteArrayInputStream(data);
                    private long pos = 0;
                    @Override
                    public long getPos() { return pos; }
                    @Override
                    public void seek(long pos) {
                        bais.reset();
                        bais.skip(pos);
                        this.pos = pos;
                    }
                    @Override
                    public int read() {
                        int b = bais.read();
                        if (b >= 0) pos++;
                        return b;
                    }
                    @Override
                    public int read(byte[] b, int off, int len) {
                        int n = bais.read(b, off, len);
                        if (n > 0) pos += n;
                        return n;
                    }
                    @Override
                    public void close() {}
                };
            }
        };
    }

    @Override
    public DorisOutputFile newOutputFile(Location location) throws IOException {
        return new DorisOutputFile() {
            @Override
            public Location location() { return location; }

            @Override
            public OutputStream create() throws IOException {
                FTPClient client = connect();
                String path = extractPath(location);
                return new OutputStream() {
                    private final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    @Override
                    public void write(int b) { baos.write(b); }
                    @Override
                    public void write(byte[] b, int off, int len) { baos.write(b, off, len); }
                    @Override
                    public void close() throws IOException {
                        if (!client.storeFile(path, new ByteArrayInputStream(baos.toByteArray()))) {
                            throw new IOException("Failed to store file: " + path);
                        }
                        client.logout();
                        client.disconnect();
                    }
                };
            }

            @Override
            public OutputStream createOrOverwrite() throws IOException {
                return create();
            }
        };
    }

    @Override
    public void close() throws IOException {}
}
