package com.salesforce.cantor.archive.s3;

import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.misc.archivable.CantorArchiver;
import com.salesforce.cantor.misc.archivable.EventsArchiver;
import com.salesforce.cantor.misc.archivable.ObjectsArchiver;
import com.salesforce.cantor.misc.archivable.SetsArchiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static com.salesforce.cantor.common.CommonPreconditions.checkArgument;

/**
 * An implementation of the archiver which stores data on an S3 instance.
 */
public class ArchiverOnS3 implements CantorArchiver {
    private static final Logger logger = LoggerFactory.getLogger(ArchiverOnS3.class);
    private static final String archivePath = "cantor-s3-archive-data";
    private static final long defaultChunkMillis = TimeUnit.HOURS.toMillis(1);

    private final SetsArchiverOnS3 setsArchive;
    private final ObjectsArchiverOnS3 objectsArchive;
    private final EventsArchiverOnS3 eventsArchive;

    public ArchiverOnS3(final Cantor cantor) throws IOException {
        checkArgument(cantor != null, "null/empty cantor");
        logger.info("initializing s3 archiver with file archive '{}'", archivePath);

        final File createDirectory = new File(archivePath);
        if (!createDirectory.mkdirs() && !createDirectory.exists()) {
            throw new IllegalStateException("Failed to create base directory for file archive: " + archivePath);
        }

        this.setsArchive = new SetsArchiverOnS3(cantor, archivePath);
        this.objectsArchive = new ObjectsArchiverOnS3(cantor, archivePath);
        this.eventsArchive = new EventsArchiverOnS3(cantor, archivePath);
    }

    @Override
    public SetsArchiver sets() {
        return this.setsArchive;
    }

    @Override
    public ObjectsArchiver objects() {
        return this.objectsArchive;
    }

    @Override
    public EventsArchiver events() {
        return this.eventsArchive;
    }
}
