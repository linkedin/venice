package com.linkedin.venice.serialization;

/**
 * Map objects to byte arrays and back again
 *
 * @param <T> The type of the object that is mapped by this serializer
 */
public interface VeniceSerializer<T> {

    /**
     * Close this serializer.
     * This method has to be idempotent if the serializer is used in KafkaProducer because it might be called
     * multiple times.
     */
    void close();

    /**
     * Construct an array of bytes from the given object
     * @param object The object
     * @return The bytes taken from the object
     */
    byte[] serialize(T object);

    /**
     * Create an object from an array of bytes
     * @param bytes An array of bytes with the objects data
     * @return A java object serialzed from the bytes
     */
    T deserialize(byte[] bytes);
}
