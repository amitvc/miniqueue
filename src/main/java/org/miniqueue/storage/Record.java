package org.miniqueue.storage;

/**
 * Represents an event that we put onto the queue.
 */
public interface Record {
    long getOffset();
    byte[] getKey();
    byte[] getValue();
}
