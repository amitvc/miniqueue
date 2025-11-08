package org.miniqueue.storage;

import org.miniqueue.util.Config;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.codahale.metrics.Counter;

/**
 * Implements EventStorage based on local disk.
 */
public class LocalDiskEventStorage implements AutoCloseable, EventStorage {

    private final FileChannel dataChannel;
    private final DataFileHeaderPage header;
    private int pageSize;
    private final AtomicLong nextAvailablePageNum;

    // Partition-scoped locks so flush/fetch for different partitions can proceed independently.
    private final Map<Short, ReentrantReadWriteLock> partitionLocks = new ConcurrentHashMap<>();

    /**
     * Opens (or creates) the data file and loads its header.
     */
    public LocalDiskEventStorage(String dataFilePath, int pageSize) throws IOException {
        this.dataChannel = FileChannel.open(Paths.get(dataFilePath),
                                            StandardOpenOption.READ,
                                            StandardOpenOption.WRITE,
                                            StandardOpenOption.CREATE);

        if (dataChannel.size() == 0) {
            // --- File is new, initialize it ---
            System.out.println("Data file is new. Initializing...");
            this.pageSize = pageSize;
            this.header = new DataFileHeaderPage(pageSize);

            // 1. Write the HeaderPage (Page 0)
            ByteBuffer headerPageBuffer = ByteBuffer.allocate(pageSize);
            header.serialize(headerPageBuffer);
            headerPageBuffer.position(0);
            headerPageBuffer.limit(pageSize);
            dataChannel.write(headerPageBuffer, 0);

            // 2. Write all 101 *empty* head metadata pages
            ByteBuffer emptyPageList = new MetaDataPage().serialize(pageSize);
            for (int i = 0; i < Config.METADATA_PAGE_COUNT; i++) {
                long pageOffset = (long) (header.getMetadataPagesStart() + i) * pageSize;
                emptyPageList.position(0); // Rewind buffer for each write
                dataChannel.write(emptyPageList, pageOffset);
            }

            // 3. Set the next available page counter
            this.nextAvailablePageNum = new AtomicLong(header.getDataPagesStart());

            // 4. Fsync everything
            dataChannel.force(true);

        } else {
            // --- File exists, read its header ---
            // We use the passed-in pageSize to read the header, then validate it.
            this.pageSize = pageSize;
            ByteBuffer headerBuffer = readPage(Config.HEADER_PAGE_NUMBER);
            this.header = new DataFileHeaderPage(headerBuffer);
            this.pageSize = this.header.getPageSize(); // Use the file's actual page size

            // 5. Set the page counter to the end of the file
            long numPages = dataChannel.size() / this.pageSize;
            this.nextAvailablePageNum = new AtomicLong(numPages);
            System.out.println("Data file loaded. Next available page: " + this.nextAvailablePageNum.get());
        }
    }

    /**
     * Fetches all records for a given partition from the DataFile.
     * Does NOT check the in-memory cache.
     * Data could be spread across multiple data pages and this will traverse all of them.
     */
    @Override
    public List<Record> getRecordsByPartition(short partitionId) throws IOException {
        Lock readLock = getPartitionLock(partitionId).readLock();
        readLock.lock();
        try {
            // 1. Get the list of ALL DataPage numbers for this partition
            List<Integer> dataPageNumbers = getAllDataPageNumbers(partitionId);

            if (dataPageNumbers.isEmpty()) {
                return new ArrayList<>();
            }

            // 2. Now, read all those DataPages
            List<Record> records = new ArrayList<>();
            for (int dataPageNum : dataPageNumbers) {
                ByteBuffer pageBuffer = readPage(dataPageNum);
                DataPage dataPage = new DataPage(pageBuffer);
                records.addAll(dataPage.getRecords());
            }
            return records;

        } finally {
            readLock.unlock();
        }
    }

    /**
     * Helper to traverse the MetaDataPage linked list and get all DataPage numbers.
     * Assumes read lock is held.
     */
    private List<Integer> getAllDataPageNumbers(short partitionId) throws IOException {
        List<Integer> dataPageNumbers = new ArrayList<>();
        int currentListPageNum = header.getMetadataPageForPartition(partitionId);

        while (currentListPageNum != 0) {
            ByteBuffer pageBuffer = readPage(currentListPageNum);
            MetaDataPage listPage = MetaDataPage.deserialize(pageBuffer);

            dataPageNumbers.addAll(listPage.getPageNumbers());
            currentListPageNum = listPage.getNextMetaDataPageNumber();
        }
        return dataPageNumbers;
    }


    /**
     * Flushes a batch of WAL entries to the data file.
     */
    public void flushWalEntries(Map<Short, List<Record>> groupedEntries, Counter flushCounter) throws IOException {
        for (Map.Entry<Short, List<Record>> batch : groupedEntries.entrySet()) {
            short partitionId = batch.getKey();
            List<Record> entries = batch.getValue();

            if (entries.isEmpty()) {
                continue;
            }

            Lock writeLock = getPartitionLock(partitionId).writeLock();
            writeLock.lock();
            try {
                flushCounter.inc(entries.size());
                // --- 1. Find the last MetaDataPage and last DataPage ---
                // This is the most complex part. We must traverse the list.
                int metadataPageForPartition = header.getMetadataPageForPartition(partitionId);
                MetaDataPage metaDataPage = MetaDataPage.deserialize(readPage(metadataPageForPartition));

                // Iterate through metadata pages if we need to. Some partitions can grow large
                // and there metadata page might need additional space.
                while (metaDataPage.getNextMetaDataPageNumber() != 0) {
                    metadataPageForPartition = metaDataPage.getNextMetaDataPageNumber();
                    metaDataPage = MetaDataPage.deserialize(readPage(metadataPageForPartition));
                }

                DataPage lastDataPage = null;
                int lastDataPageNum = metaDataPage.getLastPageNumber();
                if (lastDataPageNum != 0) {
                    // Load the last data page for the partition.
                    lastDataPage = new DataPage(readPage(lastDataPageNum));
                }

                for (Record entry : entries) {
                    Event record = new Event(entry.getOffset(), entry.getKey(), entry.getValue());

                    if (lastDataPage != null && lastDataPage.addRecord(record)) {
                        // Success! Continue to next record.
                    } else {
                        if (lastDataPage != null) {
                            writePage(lastDataPageNum, lastDataPage.getBuffer());
                        }

                        int newDataPageNum = (int) nextAvailablePageNum.getAndIncrement();
                        lastDataPageNum = newDataPageNum;
                        // Use WAL offset as LSN for the *page*
                        lastDataPage = new DataPage(pageSize, entry.getOffset());

                        if (!lastDataPage.addRecord(record)) {
                            // This should be impossible, as the page is new
                            throw new IOException("Failed to add record to a new, empty data page!");
                        }

                        if (!metaDataPage.addPageNumber(newDataPageNum, pageSize)) {
                           // MetaDataPage can become full. If it becomes full here is the protocol.
                            /**
                             *  Step 1: Get the next Available Page number.
                             *  Step 2: Create an empty MetadataPage
                             *  Step 3: Write the emppty page back to the disk
                             *  Step 4: Add the pointer to the new page in the current MetadataPage
                             *  Step 5: Save the current metadatapage
                             *  Step 6: Use the newly created metadatapage to add the new DataPage numbers.
                             */

                            final int newMetadataPageForPartition = (int) nextAvailablePageNum.getAndIncrement();
                            MetaDataPage newMetadataPage = new MetaDataPage();
                            writePage(newMetadataPageForPartition, newMetadataPage.serialize(pageSize));
                            dataChannel.force(true);

                            // Make the chain
                            metaDataPage.setNextMetaDataPageNumber(newMetadataPageForPartition);
                            writePage(metadataPageForPartition, metaDataPage.serialize(pageSize));

                            metaDataPage = newMetadataPage;
                            metadataPageForPartition = newMetadataPageForPartition;

                            metaDataPage.addPageNumber(newDataPageNum, pageSize);
                        }
                    }
                }

                if (lastDataPage != null) {
                    writePage(lastDataPageNum, lastDataPage.getBuffer());
                }
                // lastListPage is the (potentially new) last MetaDataPage
                writePage(metadataPageForPartition, metaDataPage.serialize(pageSize));
            } finally {
                writeLock.unlock();
            }
        }

        dataChannel.force(true); // We want to force sync of metadata and bytes to the disk.
    }


    private ByteBuffer readPage(int pageNum) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(pageSize);
        long offset = (long) pageNum * pageSize;
        int bytesRead = dataChannel.read(buffer, offset);

        if (bytesRead == -1) {
            throw new IOException("Read past end of file. Page: " + pageNum);
        }

        buffer.flip();
        return buffer;
    }

    private void writePage(int pageNum, ByteBuffer buffer) throws IOException {
        long offset = (long) pageNum * pageSize;
        buffer.position(0);
        buffer.limit(pageSize);
        dataChannel.write(buffer, offset);
        dataChannel.force(true);
    }

    public DataFileHeaderPage getDataFileHeader() {
        return header;
    }

    public Map<Short, List<Integer>> getAllocatedPages() throws IOException {
        Map<Short, List<Integer>> partitionPages = new java.util.HashMap<>();
        for (short i = 0; i < header.getMetadataPagesCount(); i++) {
            Lock readLock = getPartitionLock(i).readLock();
            readLock.lock();
            try {
                partitionPages.put(i, getAllDataPageNumbers(i));
            } finally {
                readLock.unlock();
            }
        }
        return partitionPages;
    }

    /**
     * Closes the file channel.
     */
    @Override
    public void close() throws IOException {
        if (dataChannel != null) {
            dataChannel.force(true);
            dataChannel.close();
        }
    }

    private ReadWriteLock getPartitionLock(short partitionId) {
        return partitionLocks.computeIfAbsent(partitionId, k -> new ReentrantReadWriteLock(true));
    }
}
