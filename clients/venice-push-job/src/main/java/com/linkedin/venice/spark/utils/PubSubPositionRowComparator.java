package com.linkedin.venice.spark.utils;

import static com.linkedin.venice.ConfigKeys.PUBSUB_TYPE_ID_TO_POSITION_CLASS_NAME_MAP;
import static com.linkedin.venice.spark.SparkConstants.KEY_COLUMN_NAME;
import static com.linkedin.venice.spark.SparkConstants.OFFSET_COLUMN_NAME;
import static com.linkedin.venice.spark.SparkConstants.POSITION_COLUMN_NAME;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.PubSubPositionDeserializer;
import com.linkedin.venice.pubsub.PubSubPositionTypeRegistry;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import org.apache.spark.sql.Row;


/**
 * Orders KIF repush rows of the same key by their position in the source topic-partition (older first).
 * <p>
 * Fast path: when both rows carry a non-negative {@link com.linkedin.venice.spark.SparkConstants#OFFSET_COLUMN_NAME},
 * rows are ordered by that numeric offset, exactly as before. Otherwise (e.g. pub-sub systems whose positions have no
 * dense numeric offset and report -1), the serialized positions in
 * {@link com.linkedin.venice.spark.SparkConstants#POSITION_COLUMN_NAME} are deserialized on the executor and compared
 * with {@link PubSubPosition#comparePositionOffline(PubSubPosition)}. If the positions cannot be ordered, a
 * {@link VeniceException} is thrown rather than silently picking an arbitrary record.
 * <p>
 * The position type registry is rebuilt on the executor from the same job property the PubSub input reader uses
 * ({@code PUBSUB_TYPE_ID_TO_POSITION_CLASS_NAME_MAP}), so no additional configuration is needed.
 */
public class PubSubPositionRowComparator implements Comparator<Row>, Serializable {
  private static final long serialVersionUID = 1L;
  private static final int MAX_KEY_BYTES_IN_ERROR = 64;

  /** Only holds {@code PUBSUB_TYPE_ID_TO_POSITION_CLASS_NAME_MAP} (if set) to keep the serialized closure small. */
  private final Properties positionTypeRegistryProps = new Properties();
  private transient PubSubPositionDeserializer positionDeserializer;

  public PubSubPositionRowComparator(VeniceProperties jobProps) {
    if (jobProps != null && jobProps.containsKey(PUBSUB_TYPE_ID_TO_POSITION_CLASS_NAME_MAP)) {
      positionTypeRegistryProps.setProperty(
          PUBSUB_TYPE_ID_TO_POSITION_CLASS_NAME_MAP,
          jobProps.getString(PUBSUB_TYPE_ID_TO_POSITION_CLASS_NAME_MAP));
    }
  }

  @Override
  public int compare(Row r1, Row r2) {
    return compare(new OrderedRow(r1), new OrderedRow(r2));
  }

  /**
   * @return the newest row (highest position) of {@code rows}, or null if {@code rows} is empty. On ties, the first
   *         such row is returned. Each position is deserialized at most once.
   */
  public Row selectLatest(List<Row> rows) {
    OrderedRow latest = null;
    for (Row row: rows) {
      OrderedRow candidate = new OrderedRow(row);
      if (latest == null || compare(candidate, latest) > 0) {
        latest = candidate;
      }
    }
    return latest == null ? null : latest.row;
  }

  /**
   * Sorts {@code rows} in place, newest (highest position) first. The sort is stable and each position is deserialized
   * at most once.
   */
  public void sortLatestFirst(List<Row> rows) {
    List<OrderedRow> orderedRows = new ArrayList<>(rows.size());
    for (Row row: rows) {
      orderedRows.add(new OrderedRow(row));
    }
    orderedRows.sort((a, b) -> compare(b, a));
    for (int i = 0; i < orderedRows.size(); i++) {
      rows.set(i, orderedRows.get(i).row);
    }
  }

  private int compare(OrderedRow r1, OrderedRow r2) {
    if (r1.offset >= 0 && r2.offset >= 0) {
      return Long.compare(r1.offset, r2.offset);
    }
    PubSubPosition position1 = r1.position();
    PubSubPosition position2 = r2.position();
    try {
      return position1.comparePositionOffline(position2);
    } catch (RuntimeException e) {
      throw new VeniceException(
          "Unable to order records of key " + keyToString(r1.row) + ": positions " + position1 + " (offset " + r1.offset
              + ") and " + position2 + " (offset " + r2.offset + ") cannot be compared offline",
          e);
    }
  }

  /** A row plus its offset and lazily deserialized position, so rows on the offset fast path never deserialize. */
  private final class OrderedRow {
    private final Row row;
    private final long offset;
    private PubSubPosition position;

    private OrderedRow(Row row) {
      this.row = row;
      this.offset = row.getAs(OFFSET_COLUMN_NAME);
    }

    private PubSubPosition position() {
      if (position == null) {
        position = toPosition(row, offset);
      }
      return position;
    }
  }

  private PubSubPosition toPosition(Row row, long offset) {
    byte[] positionBytes = row.getAs(POSITION_COLUMN_NAME);
    if (positionBytes == null) {
      throw new VeniceException(
          "Unable to order records of key " + keyToString(row) + ": offset is " + offset
              + " and no serialized position is available");
    }
    try {
      return getPositionDeserializer().toPosition(positionBytes);
    } catch (RuntimeException e) {
      throw new VeniceException(
          "Unable to order records of key " + keyToString(row) + ": failed to deserialize position (offset " + offset
              + ")",
          e);
    }
  }

  private PubSubPositionDeserializer getPositionDeserializer() {
    if (positionDeserializer == null) {
      positionDeserializer = new PubSubPositionDeserializer(
          PubSubPositionTypeRegistry.fromPropertiesOrDefault(new VeniceProperties(positionTypeRegistryProps)));
    }
    return positionDeserializer;
  }

  private static String keyToString(Row row) {
    byte[] key = row.getAs(KEY_COLUMN_NAME);
    if (key == null) {
      return "null";
    }
    int len = Math.min(key.length, MAX_KEY_BYTES_IN_ERROR);
    return "0x" + ByteUtils.toHexString(key, 0, len) + (len < key.length ? "...(" + key.length + " bytes)" : "");
  }
}
