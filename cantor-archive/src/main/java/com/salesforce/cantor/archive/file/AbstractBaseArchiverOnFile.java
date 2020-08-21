package com.salesforce.cantor.archive.file;

import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.salesforce.cantor.common.CommonPreconditions.checkArgument;

public abstract class AbstractBaseArchiverOnFile {
    protected final String baseDirectory;

    // nothing for no sub directory
    protected String subDirectory = "";

    protected AbstractBaseArchiverOnFile(final String baseDirectory) {
        this.baseDirectory = baseDirectory;
    }

    protected Path getFile(final String fileNameFormat, final Object... args) {
        return Paths.get(this.baseDirectory, this.subDirectory, String.format(fileNameFormat, args));
    }

    protected Path getArchiveLocation() {
        return Paths.get(this.baseDirectory, this.subDirectory);
    }

    protected void setSubDirectory(final String subDirectory) {
        this.subDirectory = (subDirectory != null) ? subDirectory : "";
        final Path createDirectory = getArchiveLocation();
        if (!createDirectory.toFile().exists() && !createDirectory.toFile().mkdirs()) {
            throw new IllegalStateException("Failed to create sub directory for file archive: " + createDirectory);
        }
    }

    protected void writeArchiveEntry(final ArchiveOutputStream archive, final String name, final byte[] bytes) throws IOException {
        if (bytes.length == 0) {
            return;
        }

        final TarArchiveEntry entry = new TarArchiveEntry(name);
        entry.setSize(bytes.length);
        archive.putArchiveEntry(entry);
        archive.write(bytes);
        archive.closeArchiveEntry();
    }

    protected ArchiveOutputStream getArchiveOutputStream(final Path destination) throws IOException {
        return new TarArchiveOutputStream(new GzipCompressorOutputStream(new BufferedOutputStream(Files.newOutputStream(destination))));
    }

    protected ArchiveInputStream getArchiveInputStream(final Path archiveFile) throws IOException {
        return new TarArchiveInputStream(new GzipCompressorInputStream(new BufferedInputStream(Files.newInputStream(archiveFile))));
    }

    protected boolean checkArchiveArguments(final Object instance, final String namespace, final Path destination) {
        checkArgument(instance != null, "null cantor instance, can't archive");
        checkArgument(destination != null, "null destination, can't archive");
        return Files.notExists(destination) || destination.toFile().length() == 0;
    }

    protected void checkRestoreArguments(final Object instance, final String namespace, final Path archiveFile) {
        checkArgument(instance != null, "null objects, can't restore");
        checkArgument(Files.exists(archiveFile), "can't locate archive file, can't restore: " + archiveFile);
    }
}
