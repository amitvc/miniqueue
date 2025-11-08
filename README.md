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

