package org.miniqueue.util;

/**
 * Useful configurations
 */
public class Config {
    public static final int HEADER_PAGE_NUMBER = 0;
    public static final short CURRENT_VERSION = 1;
    public static final int METADATA_PAGE_START = 1;
    public static final int METADATA_PAGE_COUNT = 101;
    public static final int DATA_PAGE_START_OFFSET = 102;
    public static final int SERVER_PORT = 3000;

    public static final int DEFAULT_PAGE_SIZE = 4096;
    public static final int MAX_PARTITION_ID = 100;
    public static final String WAL_FILE_NAME = "miniqueue.wal";
    public static final String DATA_FILE_NAME = "miniqueue.dat";
    public static final String SIGNATURE = "miniqueue";
    public static final String CHECKPOINT_FILE_PATH = "miniqueue.checkpoint";
    public static final long FLUSH_INTERVAL_MS = 500;
    public static final long COMPACTOR_INTERVAL_MS = 60_000;
    public static final long MAX_WAL_SEGMENT_SIZE = 1024  * 1024; // 1 MB WAL Segments
}
