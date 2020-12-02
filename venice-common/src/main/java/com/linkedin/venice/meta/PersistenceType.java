package com.linkedin.venice.meta;

import com.linkedin.venice.exceptions.VeniceException;


/**
 * Enums of persistence types in Venice.
 */
public enum PersistenceType {
    /**
     * Volatile storage engine based on a simple Java {@link java.util.concurrent.ConcurrentHashMap}.
     */
    IN_MEMORY,

    /**
     * Persistent storage engine that writes to durable media and maintains a B+ tree in the Java heap.
     */
    BDB,

    /**
     * Persistent storage engine that writes to durable media and maintains an off-heap in-memory index.
     */
    ROCKS_DB,

    /**
     * Fastest lock-free most secure of all storage engines. Ignores data put in it, always returns null.
     */
    BLACK_HOLE;

    private static PersistenceType[] ALL_PERSISTENCE_TYPES = values();

    public static PersistenceType getPersistenceTypeFromOrdinal(int ordinal) {
        if (ordinal >= ALL_PERSISTENCE_TYPES.length) {
            throw new VeniceException("Invalid PersistenceType ordinal: " + ordinal);
        }
        return ALL_PERSISTENCE_TYPES[ordinal];
    }
}
