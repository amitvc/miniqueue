# MiniQueue

MiniQueue is a simple, file-based queue implementation designed for learning and demonstration purposes. It provides basic message queuing functionalities using local file storage.

## Features

- **File-based Storage**: All queue data is persisted to local files.
- **WAL (Write-Ahead Log)**: Ensures data durability and recovery.
- **Checkpointing**: Periodically saves the state of the queue to optimize recovery.
- **Simple API**: Easy to integrate and use for basic queuing needs.


## Project Structure

- `src/main/java/org/miniqueue`: Contains the core Java source code.
  - `server`: Classes related to the MiniQueue server.
  - `storage`: Handles file-based storage, WAL, and checkpointing.
  - `util`: Utility classes like configuration.
- `src/test/java/org/miniqueue`: Contains unit tests for various components.

## Prereqs
- Needs java 11 and above
- Requires maven to build

## Building the Project

This project uses Maven. To build the project, navigate to the root directory and run:

```bash
mvn clean install
```

## Running the Server

To run the MiniQueue server, execute the `Main` class:

```bash
java -jar target/miniqueue-1.0-SNAPSHOT-jar-with-dependencies.jar
```

## API
Admin API
curl -X GET http://localhost:3000/admin/partitions

Fetch API - GET
curl -X GET http://localhost:3000/fetch/7


Produce API - POST
curl -X POST http://localhost:3000/produce/100 -d “Hellos:World”

## Constraints
The server maintains multiple files (less than 5) to ensure data is stored durably. Here is the list of files 
```
- miniqueue.dat 
This is data file where all entries (offset+key+value) are stored. Entries are stored in 4KB long pages. 
I have borrowed some concepts from how database store records for tables on the file system. Since there is 
no requirement of deleting data in data files we have not implemented compaction of data file. The data file 
starts with a mandatory DataFileHeaderPage. This is like the table of contents of the entire file. Please see
the structure of the header below
 +------------------+------------------+--------------------+-----------------------+-----------------------+--------------------+
 * | Signature Length | Signature Data   | Version            | Page Size             | Metadata Pages Start  | Metadata Pages Count |
 * | (short)          | (variable bytes) | (short)            | (int)                 | (int)                 | (int)              |
 * | 2 bytes          | (size=SIGNATURE) | 2 bytes            | 4 bytes               | 4 bytes               | 4 bytes            |
 * +------------------+------------------+--------------------+-----------------------+-----------------------+--------------------+
 * | Data Pages Start |
 * | (int)            |
 * | 4 bytes          |
 * +------------------+
 
 MetaDataPage is one that holds the actual information of which pages are being allocated for the given partition. Since we allow 100 partions
 there are 100 MetaDataPages. If a single MetaDataPage cannot hold all references to the pages allocated for a partition a new MetaDataPage
 is created and pointer to it is kept in the current MetaDataPage. Think of it as LinkedList of MetadataPages. See structure below
 +-------------------------+------------------------+-------------------------+
| Next MetaData Page Num  | Page Count             | Page Numbers (list)     |
| (int)                   | (int)                  | (int[n]...)             |
| 4 bytes                 | 4 bytes                | 4 bytes * pageCount     |
+-------------------------+------------------------+-------------------------+

DataPage is where we store the raw bytes of the entries belonging to the partition. See the structure below
+--------------------+--------------------+--------------------+-------------------------------+
| Page LSN           | Current Data End   | Record Count       | Raw Page Data (buffer)        |
| (long)             | (int)              | (int)              | (byte[pageSize - 16])         |
| 8 bytes            | 4 bytes            | 4 bytes            | (variable, depends on pageSize)|
+--------------------+--------------------+--------------------+-------------------------------+
```

```
miniqueue.checkpoint
This file maintains the offsets we have written to the data file for each partition. The purpose of this file is so we can trim
the WAL once the data from WAL is flushed to the data file.
+----------------------+-----------------------+-----------------------+-----------------------+-----------------------+
| Entry Count (int)    | Partition ID (short)  | Flushed Offset (long) | Partition ID (short)  | Flushed Offset (long) |
| 4 bytes              | 2 bytes               | 8 bytes               | 2 bytes               | 8 bytes               |
+----------------------+-----------------------+-----------------------+-----------------------+-----------------------+
```

```dtd
miniqueue.wal directory is maintained for the WAL segments. WAL is where the entries are first persisted. We use the WAL to simplify 
our write operations and make them fast. Writing to a file sequentially is much faster than writing data at random locations. 
The WAL helps with our durability story as well since we can replay WAL on startup to catchup if the system had crashed prior to flushing 
data to data pages. The layout of the WAL file is simple. It contains WAL entries and each wal entry is structured as below
+----------------+----------------+----------------+----------------+----------------+----------------+----------------+
| Record Length  | Partition ID   | Offset         | Key Length     | Key Data       | Value Length   | Value Data     |
| (short)        | (short)        | (long)         | (short)        | (bytes...)     | (short)        | (bytes...)     |
| 2 bytes        | 2 bytes        | 8 bytes        | 2 bytes        | (KeyLength)    | 2 bytes        | (ValueLength)  |
+----------------+----------------+----------------+----------------+----------------+----------------+----------------+

Once the system identifies that all offsets in the current wal are flushed to data pages the WAL is closed and new 
WAL log is created. This way we don't have an ever increasing WAL log.
```

## Performance characteristics
I am batching WAL records and flushing them in groups to the disk. Earlier I was flushing each record to disk and that did not peform
well as I scaled up the tests. Hence I introduced AsyncWalWriter which batches the WAL records and writes them. There are 
settings in Config.java that need to be played around with in order to achieve optimal performance based on the work load. One of the
todo's is to expose these while we launch the service so it can be changed without requiring a recompile. 
The settings of importance are:
```
public static final long COMPACTOR_INTERVAL_MS = 10_000; // How often we compact the wal logs. Important since we dont 
want to increase our file count more than 5

The ones below are important to give a really good write throughput
public static final long MAX_WAL_SEGMENT_SIZE = 5*1024  * 1024; // 5 MB WAL Segments
public static final int WAL_MAX_BATCH_SIZE = 64; // keep this small enough based on risk tolerance if system crashes before WAL is flushed.
public static final long WAL_FLUSH_INTERVAL_MS = 5; // milliseconds
public static final long FLUSH_INTERVAL_MS = 2000; // This is also important. This setting flushes the in-memory cache to the 
data files. That ensures entries written up in WAL are now in the data pages as well.

Some performance numbers;

Produce test harness:
--- MiniQueue Performance Test ---
Configuration: 10 Threads, 300000 Messages/Thread
Total Messages: 3000000

Starting all producer threads...
...All threads complete.

--- Performance Report ---
Total Time:     160.24 seconds
Total Messages: 3000000
Throughput:     18721.50 Msgs/sec
Req Success:     3000000 Msgs/sec
Req Failed:      0 Msgs/sec

--- Latency Report (ms) ---
p50 (Median): 0.32 ms
p95:          0.46 ms
p99:          0.71 ms
p99.9:        3.95 ms
Max:          31.10 ms

--- MiniQueue Fetch Performance Test ---
Threads=10  Fetches/Thread=2000
Targeting 101 populated partitions.
Starting fetch threads...
All fetch threads completed.

--- Fetch Performance Report ---
Total Requests: 20000
Total Time: 45.84 s
Throughput: 436.33 req/s
Successes: 20000
Failures: 0

--- Fetch Latency (ms) ---
p50: 16.02
p95: 49.64
p99: 73.31
p99.9: 2018.61
Max: 2038.35


Metrics from the service during the run:
compaction-counter
             count = 20
fetch-requests
             count = 20000
flush-counter
             count = 2253205
produce-requests
             count = 3000000

-- Timers ----------------------------------------------------------------------
fetch-handler
             count = 20000
         mean rate = 55.52 calls/second
     1-minute rate = 4.27 calls/second
     5-minute rate = 27.82 calls/second
    15-minute rate = 16.60 calls/second
               min = 0.00 milliseconds
               max = 611.19 milliseconds
              mean = 1.74 milliseconds
            stddev = 15.97 milliseconds
            median = 0.00 milliseconds
              75% <= 0.01 milliseconds
              95% <= 14.62 milliseconds
              98% <= 15.09 milliseconds
              99% <= 15.36 milliseconds
            99.9% <= 34.68 milliseconds
produce-handler
             count = 3000000
         mean rate = 8326.94 calls/second
     1-minute rate = 2277.11 calls/second
     5-minute rate = 5239.20 calls/second
    15-minute rate = 2682.40 calls/second
               min = 0.01 milliseconds
               max = 0.05 milliseconds
              mean = 0.01 milliseconds
            stddev = 0.00 milliseconds
            median = 0.01 milliseconds
              75% <= 0.01 milliseconds
              95% <= 0.02 milliseconds
              98% <= 0.02 milliseconds
              99% <= 0.03 milliseconds
            99.9% <= 0.05 milliseconds

```
