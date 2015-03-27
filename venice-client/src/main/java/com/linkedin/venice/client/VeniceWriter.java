package com.linkedin.venice.client;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.producer.KafkaProducer;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.message.KafkaValue;
import com.linkedin.venice.message.OperationType;
import com.linkedin.venice.serialization.Serializer;
import com.linkedin.venice.utils.Props;
import org.apache.log4j.Logger;


/**
 * Class which acts as the primary writer API
 */
public class VeniceWriter<K, V> {

    // log4j logger
    static final Logger logger = Logger.getLogger(VeniceWriter.class.getName());

    protected final KafkaProducer producer;

    protected Props props;
    protected final String storeName;
    protected final Serializer<K> keySerializer;
    protected final Serializer<V> valueSerializer;


    public VeniceWriter(Props props, String storeName, Serializer<K> keySerializer, Serializer<V> valueSerializer) {

        try {
            this.props = props;
            this.storeName = storeName;
            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;
        } catch (Exception e) {
            logger.error("Error while starting up configuration for VeniceWriter.", e);
            throw new VeniceException("Error while starting up configuration for VeniceWriter", e);
        }

        producer = new KafkaProducer(props);
    }

    /**
     * Execute a standard "delete" on the key.
     *
     * @param key - The key to delete in storage.
     */
    public void delete(K key) {
        // Ensure the Operation type for KafkaKey is WRITE. And the actual Operation type DELETE is used in KafkaValue
        KafkaKey kafkaKey = new KafkaKey(OperationType.WRITE, keySerializer.toBytes(key));
        KafkaValue kafkaValue = new KafkaValue(OperationType.DELETE);
        producer.sendMessage(storeName, kafkaKey, kafkaValue);
    }

    /**
     * Execute a standard "put" on the key.
     *
     * @param key   - The key to put in storage.
     * @param value - The value to be associated with the given key
     */
    public void put(K key, V value) {
        // Ensure the Operation type for KafkaKey is WRITE. And the actual Operation type PUT is used in KafkaValue
        KafkaKey kafkaKey = new KafkaKey(OperationType.WRITE, keySerializer.toBytes(key));
        KafkaValue kafkaValue = new KafkaValue(OperationType.PUT, valueSerializer.toBytes(value));
        producer.sendMessage(storeName, kafkaKey, kafkaValue);
    }

    /**
     * Execute a standard "partial put" on the key.
     *
     * @param key   - The key to put in storage.
     * @param value - The value to be associated with the given key
     */
    public void putPartial(K key, V value) {

        throw new UnsupportedOperationException("Partial put is not supported yet.");
    }

    public void close() {
        producer.close();
    }
}
