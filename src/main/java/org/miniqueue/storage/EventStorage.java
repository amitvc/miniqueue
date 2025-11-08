package org.miniqueue.storage;

import java.io.IOException;
import java.util.List;

public interface EventStorage {

    List<Record> getRecordsByPartition(short partitionId) throws IOException;
}
