package org.apache.doris.filesystem.sftp;

import org.apache.doris.filesystem.DorisInputFile;
import org.apache.doris.filesystem.DorisInputStream;
import org.apache.doris.filesystem.DorisOutputFile;
import org.apache.doris.filesystem.FileEntry;
import org.apache.doris.filesystem.FileIterator;
import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.Location;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Vector;

public class SftpFileSystem implements FileSystem {

    private final Map<String, String> properties;
    private final String host;
    private int port = 22;
    private final String user;
    private final String password;
    private final String sshKey;

    public SftpFileSystem(Map<String, String> properties) {
        this.properties = properties;
        String uri = properties.getOrDefault("uri", "");
        URI parsed = URI.create(uri);
        this.host = parsed.getHost();
        if (parsed.getPort() > 0) {
            this.port = parsed.getPort();
        }
        this.user = properties.getOrDefault("user", "");
        this.password = properties.getOrDefault("password", "");
        this.sshKey = properties.getOrDefault("ssh_key", "");
    }

    private Session createSession() throws JSchException {
        JSch jsch = new JSch();
        if (!sshKey.isEmpty()) {
            jsch.addIdentity(sshKey);
        }
        Session session = jsch.getSession(user, host, port);
        if (!password.isEmpty()) {
            session.setPassword(password);
        }
        session.setConfig("StrictHostKeyChecking", "no");
        return session;
    }

    private ChannelSftp connect() throws IOException {
        try {
            Session session = createSession();
            session.connect();
            Channel channel = session.openChannel("sftp");
            channel.connect();
            return (ChannelSftp) channel;
        } catch (JSchException e) {
            throw new IOException("SFTP connection failed: " + e.getMessage(), e);
        }
    }

    private void disconnect(ChannelSftp sftp) {
        if (sftp != null) {
            Session session = null;
            try {
                session = sftp.getSession();
            } catch (JSchException ignored) {}
            sftp.disconnect();
            if (session != null) {
                session.disconnect();
            }
        }
    }

    private String extractPath(Location location) {
        String uri = location.uri();
        URI parsed = URI.create(uri);
        String path = parsed.getPath();
        return path != null ? path : "/";
    }

    @Override
    public boolean exists(Location location) throws IOException {
        ChannelSftp sftp = connect();
        try {
            sftp.stat(extractPath(location));
            return true;
        } catch (SftpException e) {
            if (e.id == ChannelSftp.SSH_FX_NO_SUCH_FILE) {
                return false;
            }
            throw new IOException("SFTP exists check failed: " + e.getMessage(), e);
        } finally {
            disconnect(sftp);
        }
    }

    @Override
    public void mkdirs(Location location) throws IOException {
        ChannelSftp sftp = connect();
        try {
            mkdirsRecursive(sftp, extractPath(location));
        } finally {
            disconnect(sftp);
        }
    }

    private void mkdirsRecursive(ChannelSftp sftp, String path) throws IOException {
        try {
            sftp.stat(path);
        } catch (SftpException e) {
            if (e.id == ChannelSftp.SSH_FX_NO_SUCH_FILE) {
                String parent = path.substring(0, path.lastIndexOf('/'));
                if (!parent.isEmpty() && !parent.equals("/")) {
                    mkdirsRecursive(sftp, parent);
                }
                try {
                    sftp.mkdir(path);
                } catch (SftpException ex) {
                    throw new IOException("Failed to create directory: " + path, ex);
                }
            }
        }
    }

    @Override
    public void delete(Location location, boolean recursive) throws IOException {
        ChannelSftp sftp = connect();
        try {
            sftp.rm(extractPath(location));
        } catch (SftpException e) {
            throw new IOException("SFTP delete failed: " + e.getMessage(), e);
        } finally {
            disconnect(sftp);
        }
    }

    @Override
    public void rename(Location src, Location dst) throws IOException {
        ChannelSftp sftp = connect();
        try {
            sftp.rename(extractPath(src), extractPath(dst));
        } catch (SftpException e) {
            throw new IOException("SFTP rename failed: " + e.getMessage(), e);
        } finally {
            disconnect(sftp);
        }
    }

    @Override
    public FileIterator list(Location location) throws IOException {
        ChannelSftp sftp = connect();
        try {
            @SuppressWarnings("unchecked")
            Vector<ChannelSftp.LsEntry> entries = sftp.ls(extractPath(location));
            List<FileEntry> result = new ArrayList<>();
            for (ChannelSftp.LsEntry entry : entries) {
                String name = entry.getFilename();
                if (".".equals(name) || "..".equals(name)) {
                    continue;
                }
                String childUri = location.uri().replaceAll("/+$", "") + "/" + name;
                result.add(new FileEntry(
                    Location.of(childUri),
                    entry.getAttrs().getSize(),
                    entry.getAttrs().isDir(),
                    (long) entry.getAttrs().getMTime() * 1000L,
                    null
                ));
            }
            return new FileIterator() {
                private int idx = 0;
                @Override
                public boolean hasNext() { return idx < result.size(); }
                @Override
                public FileEntry next() { return result.get(idx++); }
                @Override
                public void close() {}
            };
        } catch (SftpException e) {
            throw new IOException("SFTP list failed: " + e.getMessage(), e);
        } finally {
            disconnect(sftp);
        }
    }

    @Override
    public DorisInputFile newInputFile(Location location) throws IOException {
        return new DorisInputFile() {
            @Override
            public Location location() { return location; }

            @Override
            public long length() throws IOException {
                ChannelSftp sftp = connect();
                try {
                    return sftp.stat(extractPath(location)).getSize();
                } catch (SftpException e) {
                    throw new IOException("SFTP stat failed: " + e.getMessage(), e);
                } finally {
                    disconnect(sftp);
                }
            }

            @Override
            public boolean exists() throws IOException {
                return SftpFileSystem.this.exists(location);
            }

            @Override
            public long lastModifiedTime() throws IOException {
                ChannelSftp sftp = connect();
                try {
                    return (long) sftp.stat(extractPath(location)).getMTime() * 1000L;
                } catch (SftpException e) {
                    throw new IOException("SFTP stat failed: " + e.getMessage(), e);
                } finally {
                    disconnect(sftp);
                }
            }

            @Override
            public DorisInputStream newStream() throws IOException {
                ChannelSftp sftp = connect();
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                try (InputStream is = sftp.get(extractPath(location))) {
                    byte[] buf = new byte[8192];
                    int n;
                    while ((n = is.read(buf)) != -1) {
                        baos.write(buf, 0, n);
                    }
                } catch (SftpException e) {
                    throw new IOException("SFTP get failed: " + e.getMessage(), e);
                }
                byte[] data = baos.toByteArray();
                disconnect(sftp);
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
                ChannelSftp sftp = connect();
                return new OutputStream() {
                    private final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    @Override
                    public void write(int b) { baos.write(b); }
                    @Override
                    public void write(byte[] b, int off, int len) { baos.write(b, off, len); }
                    @Override
                    public void close() throws IOException {
                        try {
                            sftp.put(new ByteArrayInputStream(baos.toByteArray()), extractPath(location));
                        } catch (SftpException e) {
                            throw new IOException("SFTP put failed: " + e.getMessage(), e);
                        } finally {
                            disconnect(sftp);
                        }
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
