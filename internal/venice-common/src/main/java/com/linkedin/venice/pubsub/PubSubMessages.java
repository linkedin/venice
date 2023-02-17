package com.linkedin.venice.pubsub;

import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.utils.AbstractIterator;


public class PubSubMessages<K, V, OFFSET> implements Iterable<PubSubMessage<K, V, OFFSET>> {
  public static final PubSubMessages<Object, Object, Object> EMPTY = new PubSubMessages<>(Collections.EMPTY_MAP);

  private final Map<PubSubTopicPartition, List<PubSubMessage<K, V, OFFSET>>> messages;

  public PubSubMessages(Map<PubSubTopicPartition, List<PubSubMessage<K, V, OFFSET>>> messages) {
    this.messages = messages;
  }

  /**
   * Get just the records for the given partition
   *
   * @param partition The partition to get records for
   */
  public List<PubSubMessage<K, V, OFFSET>> messages(PubSubTopicPartition partition) {
    List<PubSubMessage<K, V, OFFSET>> recs = this.messages.get(partition);
    if (recs == null)
      return Collections.emptyList();
    else
      return Collections.unmodifiableList(recs);
  }

  /**
   * Get just the records for the given topic
   */
  public Iterable<PubSubMessage<K, V, OFFSET>> messages(PubSubTopic topic) {
    if (topic == null)
      throw new IllegalArgumentException("Topic must be non-null.");
    List<List<PubSubMessage<K, V, OFFSET>>> recs = new ArrayList<>();
    for (Map.Entry<PubSubTopicPartition, List<PubSubMessage<K, V, OFFSET>>> entry: messages.entrySet()) {
      if (entry.getKey().getPubSubTopic().equals(topic))
        recs.add(entry.getValue());
    }
    return new ConcatenatedIterable<>(recs);
  }

  /**
   * Get the partitions which have records contained in this record set.
   * @return the set of partitions with data in this record set (may be empty if no data was returned)
   */
  public Set<PubSubTopicPartition> pubSubTopicPartitions() {
    return Collections.unmodifiableSet(messages.keySet());
  }

  @Override
  public Iterator<PubSubMessage<K, V, OFFSET>> iterator() {
    return new ConcatenatedIterable<>(messages.values()).iterator();
  }

  /**
   * The number of records for all topics
   */
  public int count() {
    int count = 0;
    for (List<PubSubMessage<K, V, OFFSET>> recs: this.messages.values())
      count += recs.size();
    return count;
  }

  private static class ConcatenatedIterable<K, V, OFFSET> implements Iterable<PubSubMessage<K, V, OFFSET>> {
    private final Iterable<? extends Iterable<PubSubMessage<K, V, OFFSET>>> iterables;

    public ConcatenatedIterable(Iterable<? extends Iterable<PubSubMessage<K, V, OFFSET>>> iterables) {
      this.iterables = iterables;
    }

    @Override
    public Iterator<PubSubMessage<K, V, OFFSET>> iterator() {
      return new AbstractIterator<PubSubMessage<K, V, OFFSET>>() {
        Iterator<? extends Iterable<PubSubMessage<K, V, OFFSET>>> iters = iterables.iterator();
        Iterator<PubSubMessage<K, V, OFFSET>> current;

        public PubSubMessage<K, V, OFFSET> makeNext() {
          while (current == null || !current.hasNext()) {
            if (iters.hasNext())
              current = iters.next().iterator();
            else
              return allDone();
          }
          return current.next();
        }
      };
    }
  }

  public boolean isEmpty() {
    return messages.isEmpty();
  }

  public static <K, V, OFFSET> PubSubMessages<K, V, OFFSET> empty() {
    return (PubSubMessages<K, V, OFFSET>) EMPTY;
  }

}
