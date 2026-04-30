package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link KafkaConsumerService#convertTopicPartitionIngestionInfoMapToStr}.
 *
 * <p>The formatter renders a per-consumer ingestion-info map as a fixed-width table for logging.
 * These tests exercise edge cases (null/empty input, single row, mixed lag, long partition names,
 * unusual values) and verify that the output is sorted, aligned, and that the optional triggering
 * partition is marked exactly once on the right row.
 */
public class KafkaConsumerServiceFormatterTest {
  private static final String CONSUMER_ID = "shared-consumer-0";
  private static final long LAST_POLL_MS = 53694L;

  private final PubSubTopicRepository topicRepository = new PubSubTopicRepository();

  private PubSubTopicPartition tp(String topicName, int partition) {
    PubSubTopic topic = topicRepository.getTopic(topicName);
    return new PubSubTopicPartitionImpl(topic, partition);
  }

  private TopicPartitionIngestionInfo info(long lag, long latestOffset, double msgRate, double byteRate, long lastRec) {
    return infoVt(lag, latestOffset, msgRate, byteRate, lastRec, "version-topic");
  }

  /** Variant where the per-row versionTopic name matters (e.g. hybrid-leader case). */
  private TopicPartitionIngestionInfo infoVt(
      long lag,
      long latestOffset,
      double msgRate,
      double byteRate,
      long lastRec,
      String versionTopicName) {
    return new TopicPartitionIngestionInfo(
        latestOffset,
        lag,
        msgRate,
        byteRate,
        CONSUMER_ID,
        LAST_POLL_MS,
        lastRec,
        versionTopicName);
  }

  private static String fmt(Map<PubSubTopicPartition, TopicPartitionIngestionInfo> map, PubSubTopicPartition trigger) {
    return KafkaConsumerService.convertTopicPartitionIngestionInfoMapToStr(map, trigger);
  }

  @Test
  public void nullOrEmptyMapReturnsEmptyString() {
    Assert.assertEquals(fmt(null, null), "");
    Assert.assertEquals(fmt(Collections.emptyMap(), null), "");
  }

  /**
   * A single-entry dump exercises three things at once: the consumer-level header, the column
   * header (ordering + every column title), and a data row with formatted values. Verifying them
   * here keeps three small tests' worth of coverage in one focused test.
   */
  @Test
  public void singleEntryRendersHeaderColumnHeaderAndDataRow() {
    Map<PubSubTopicPartition, TopicPartitionIngestionInfo> map = new HashMap<>();
    map.put(tp("store_v1", 0), info(42, 100, 1.5, 256.0, 1000));
    String[] lines = fmt(map, null).split("\n");

    Assert.assertEquals(lines.length, 3, "expect header, column-header, one row");

    // Consumer-level fields hoisted into header.
    Assert.assertTrue(lines[0].contains("consumer=" + CONSUMER_ID), lines[0]);
    Assert.assertTrue(lines[0].contains("lastPoll=" + LAST_POLL_MS + "ms"), lines[0]);
    Assert.assertTrue(lines[0].contains("partitions=1"), lines[0]);

    // Column header: every title appears in the documented order.
    int[] indices = { lines[1].indexOf("partition"), lines[1].indexOf("lag"), lines[1].indexOf("msgRate"),
        lines[1].indexOf("byteRate"), lines[1].indexOf("lastRecord(ms)"), lines[1].indexOf("latestOffset"),
        lines[1].indexOf("versionTopic") };
    for (int i = 0; i < indices.length; i++) {
      Assert.assertTrue(indices[i] >= 0, "missing column at position " + i + ": " + lines[1]);
      if (i > 0) {
        Assert.assertTrue(indices[i] > indices[i - 1], "wrong column order: " + lines[1]);
      }
    }

    // Data row: partition name, lag, and 2-decimal rate formatting.
    Assert.assertTrue(lines[2].contains("store_v1-0"), lines[2]);
    Assert.assertTrue(lines[2].contains("42"), lines[2]);
    Assert.assertTrue(lines[2].contains("1.50"), "msgRate must format as 1.50: " + lines[2]);
    Assert.assertTrue(lines[2].contains("256.00"), "byteRate must format as 256.00: " + lines[2]);
  }

  @Test
  public void rowsAreSortedByLagDescending() {
    // LinkedHashMap with deliberately wrong insertion order so iteration order can't satisfy the test.
    Map<PubSubTopicPartition, TopicPartitionIngestionInfo> map = new LinkedHashMap<>();
    map.put(tp("a_v1", 0), info(0, 100, 0, 0, 100));
    map.put(tp("b_v1", 0), info(5_000_000, 200, 0, 0, 100));
    map.put(tp("c_v1", 0), info(50, 300, 0, 0, 100));
    String[] rows = dataRows(fmt(map, null));

    Assert.assertEquals(rows.length, 3);
    Assert.assertTrue(rows[0].contains("b_v1-0"), "highest lag first: " + rows[0]);
    Assert.assertTrue(rows[1].contains("c_v1-0"), "mid lag second: " + rows[1]);
    Assert.assertTrue(rows[2].contains("a_v1-0"), "zero lag last: " + rows[2]);
  }

  @Test
  public void equalLagRowsAreOrderedDeterministicallyByPartitionName() {
    // All four rows have offsetLag=0 — common in production when a consumer is idle.
    // Without a tie-breaker, HashMap iteration order would vary across JVMs and runs.
    // Verify the formatter sorts equal-lag rows by partition name ascending so output is stable.
    Map<PubSubTopicPartition, TopicPartitionIngestionInfo> input = new HashMap<>();
    input.put(tp("zeta_v1", 0), info(0, 0, 0, 0, 0));
    input.put(tp("alpha_v1", 0), info(0, 0, 0, 0, 0));
    input.put(tp("mu_v1", 0), info(0, 0, 0, 0, 0));
    input.put(tp("beta_v1", 0), info(0, 0, 0, 0, 0));

    // Run the formatter twice on the same input; rows must come out in the same order.
    String[] firstRun = dataRows(fmt(input, null));
    String[] secondRun = dataRows(fmt(input, null));
    Assert.assertEquals(secondRun, firstRun, "tie-broken sort must be deterministic across runs");

    // And the order must be partition-name ascending (alpha, beta, mu, zeta).
    Assert.assertTrue(firstRun[0].contains("alpha_v1-0"), firstRun[0]);
    Assert.assertTrue(firstRun[1].contains("beta_v1-0"), firstRun[1]);
    Assert.assertTrue(firstRun[2].contains("mu_v1-0"), firstRun[2]);
    Assert.assertTrue(firstRun[3].contains("zeta_v1-0"), firstRun[3]);
  }

  @Test
  public void triggeringPartitionIsMarkedExactlyOnce() {
    Map<PubSubTopicPartition, TopicPartitionIngestionInfo> map = new HashMap<>();
    map.put(tp("a_v1", 0), info(100, 0, 0, 0, 0));
    PubSubTopicPartition trigger = tp("b_v1", 0);
    map.put(trigger, info(0, 0, 0, 0, 0));
    map.put(tp("c_v1", 0), info(50, 0, 0, 0, 0));
    String out = fmt(map, trigger);

    Assert.assertEquals(occurrences(out, "  * "), 1, "exactly one row must carry marker:\n" + out);
    Assert.assertEquals(occurrences(out, "(triggered)"), 1, "exactly one row must carry annotation:\n" + out);
    for (String row: dataRows(out)) {
      if (row.startsWith("  * ")) {
        Assert.assertTrue(row.contains("b_v1-0"), "marker on wrong row: " + row);
        Assert.assertTrue(row.endsWith("(triggered)"), "trigger row missing annotation: " + row);
      }
    }
  }

  @Test
  public void noMarkerWhenTriggerIsNullOrAbsentFromMap() {
    Map<PubSubTopicPartition, TopicPartitionIngestionInfo> map = new HashMap<>();
    map.put(tp("a_v1", 0), info(100, 0, 0, 0, 0));
    map.put(tp("b_v1", 0), info(0, 0, 0, 0, 0));
    for (PubSubTopicPartition trigger: new PubSubTopicPartition[] { null, tp("not_in_map_v1", 99) }) {
      String out = fmt(map, trigger);
      Assert.assertFalse(out.contains("(triggered)"), "no marker expected; trigger=" + trigger + ":\n" + out);
      Assert.assertFalse(out.contains("  * "), "no asterisk expected; trigger=" + trigger + ":\n" + out);
      for (String row: dataRows(out)) {
        Assert.assertTrue(row.startsWith("    "), "row must use 4-space margin without trigger: " + row);
      }
    }
  }

  @Test
  public void onlyTriggerRowGetsMarkerEvenIfPartitionNumbersOverlap() {
    // Defensive: a partition with the same partition number on a different topic must not be
    // mistaken for the trigger (matches by full PubSubTopicPartition.equals, not partition number).
    Map<PubSubTopicPartition, TopicPartitionIngestionInfo> map = new LinkedHashMap<>();
    map.put(tp("alpha_v1", 5), info(100, 0, 0, 0, 0));
    map.put(tp("beta_v1", 5), info(50, 0, 0, 0, 0));
    String out = fmt(map, tp("alpha_v1", 5));

    Assert.assertEquals(occurrences(out, "(triggered)"), 1);
    for (String row: dataRows(out)) {
      if (row.contains("alpha_v1-5")) {
        Assert.assertTrue(row.contains("(triggered)"), row);
      } else if (row.contains("beta_v1-5")) {
        Assert.assertFalse(row.contains("(triggered)"), row);
      }
    }
  }

  @Test
  public void columnsAlignAcrossRowsRegardlessOfDataWidth() {
    // Mix tiny and very large values to force column-width adaptation.
    Map<PubSubTopicPartition, TopicPartitionIngestionInfo> map = new LinkedHashMap<>();
    map.put(tp("a_v1", 0), info(1, 1, 0.0, 0.0, 1));
    map.put(
        tp("very_long_store_name_with_many_chars_v999", 12345),
        info(999_999_999L, 999_999_999L, 5.5, 7777.77, 9_999_999L));
    map.put(tp("m_v2", 7), info(1000, 50000, 0.5, 100.0, 5000));
    String[] lines = fmt(map, null).split("\n");

    // Alignment invariant: column header and every non-trigger data row are the same length.
    int width = lines[1].length();
    for (int i = 2; i < lines.length; i++) {
      String row = lines[i].endsWith("(triggered)")
          ? lines[i].substring(0, lines[i].length() - "  (triggered)".length())
          : lines[i];
      Assert.assertEquals(row.length(), width, "row " + i + " width mismatch: '" + row + "'");
    }
  }

  @Test
  public void widthsAdaptToHeaderWhenDataIsSmaller() {
    // Header titles "latestOffset"=12, "lastRecord(ms)"=14, "versionTopic"=12 should dominate.
    Map<PubSubTopicPartition, TopicPartitionIngestionInfo> map = new HashMap<>();
    map.put(tp("a_v1", 0), infoVt(0, 1, 0, 0, 0, "vt"));
    String[] lines = fmt(map, null).split("\n");
    Assert.assertEquals(
        lines[2].length(),
        lines[1].length(),
        "row should pad to match wider header titles; row='" + lines[2] + "' header='" + lines[1] + "'");
  }

  @Test
  public void consumerLevelFieldsAppearOnlyInHeader() {
    Map<PubSubTopicPartition, TopicPartitionIngestionInfo> map = new HashMap<>();
    for (int i = 0; i < 5; i++) {
      map.put(tp("store_v1", i), info(i, 100 + i, 0, 0, 1000));
    }
    String out = fmt(map, null);

    Assert.assertEquals(occurrences(out, CONSUMER_ID), 1, "consumer id should appear exactly once");
    Assert.assertEquals(occurrences(out, "lastPoll="), 1, "lastPoll should appear exactly once");
    // Old key:value form must not leak through.
    Assert.assertFalse(out.contains("consumerIdStr:"), out);
    Assert.assertFalse(out.contains("elapsedTimeSinceLastConsumerPollInMs:"), out);
    Assert.assertFalse(out.contains("versionTopicName:"), out);
  }

  @Test
  public void headerReflectsFirstRowAfterSort() {
    // The formatter pulls consumer-level fields from rows.get(0) (worst-lag row after sort).
    // Document this behavior in case rows ever carry inconsistent consumer fields.
    Map<PubSubTopicPartition, TopicPartitionIngestionInfo> map = new LinkedHashMap<>();
    map.put(tp("a_v1", 0), new TopicPartitionIngestionInfo(100, 1, 0, 0, "consumer-low", 67890L, 0, "vt")); // lower lag
    map.put(tp("b_v1", 0), new TopicPartitionIngestionInfo(100, 999, 0, 0, "consumer-high", 12345L, 0, "vt")); // wins
    String firstLine = fmt(map, null).split("\n")[0];

    Assert.assertTrue(firstLine.contains("consumer-high"), firstLine);
    Assert.assertTrue(firstLine.contains("12345"), firstLine);
  }

  @Test
  public void versionTopicColumnDistinguishesHybridLeaderFromBatch() {
    // Hybrid leader past EOP: partition is on the realtime topic but the consumer hydrates a
    // versioned topic. The partition name alone doesn't reveal which version is consuming, so
    // versionTopic column must surface that mapping.
    Map<PubSubTopicPartition, TopicPartitionIngestionInfo> map = new LinkedHashMap<>();
    map.put(tp("store1_rt", 0), infoVt(100, 0, 0, 0, 0, "store1_v3"));
    map.put(tp("store1_v3", 0), infoVt(0, 0, 0, 0, 0, "store1_v3"));
    String[] rows = dataRows(fmt(map, null));

    int rtRow = rowIndexContaining(rows, "store1_rt-0");
    int batchRow = rowIndexContaining(rows, "store1_v3-0");
    Assert.assertTrue(rtRow >= 0 && batchRow >= 0, "both rows must be present");
    Assert.assertTrue(rows[rtRow].contains("store1_v3"), "rt row must surface consuming version: " + rows[rtRow]);
    Assert
        .assertTrue(rows[batchRow].contains("store1_v3"), "batch row must still print versionTopic: " + rows[batchRow]);
  }

  @Test
  public void nullVersionTopicRendersAsBlankNotLiteralNull() {
    // Defensive: producing path coerces null to "" already, but the formatter must also handle null.
    Map<PubSubTopicPartition, TopicPartitionIngestionInfo> map = new HashMap<>();
    map.put(tp("store_v1", 0), infoVt(0, 1, 0, 0, 0, null));
    Assert.assertFalse(fmt(map, null).contains("null"), "null versionTopic must not leak as literal 'null'");
  }

  @Test
  public void ratesFormatToTwoDecimalsIncludingNegativeAndZero() {
    Map<PubSubTopicPartition, TopicPartitionIngestionInfo> map = new HashMap<>();
    map.put(tp("a_v1", 0), info(0, 0, 0.0, 0.0, 0));
    map.put(tp("b_v1", 0), info(0, 0, -1.0, -1.0, 0));
    String out = fmt(map, null);
    Assert.assertTrue(out.contains("0.00"), out);
    Assert.assertTrue(out.contains("-1.00"), out);
  }

  /**
   * End-to-end scenario covering 16 partitions across 3 stores sharing a single consumer (the
   * shape that motivated this change). Verifies (a) sort order, (b) trigger marker placement,
   * (c) versionTopic column, and (d) that messy float values like 5.139... and 735.749... round
   * to 2 decimals. Store names are synthetic — `storeA` is the high-lag cluster, `storeB` is
   * the mid-lag cluster with one row that exercises non-zero rate formatting, and `storeC` is
   * the low-lag cluster that contains the trigger row.
   */
  @Test
  public void realisticSharedConsumerScenario() {
    String storeA = "storeA_v3";
    String storeB = "storeB_v2";
    String storeC = "storeC_v89";
    Map<PubSubTopicPartition, TopicPartitionIngestionInfo> map = new LinkedHashMap<>();
    map.put(tp(storeA, 7), infoVt(3971738, 4000961, 0, 0, 385294, storeA));
    map.put(tp(storeB, 84), infoVt(0, 5346109, 0, 0, 385301, storeB));
    PubSubTopicPartition trigger = tp(storeC, 40);
    map.put(trigger, infoVt(0, 8232319, 0, 0, 1226927, storeC));
    map.put(tp(storeC, 10), infoVt(0, 8235941, 0, 0, 268715, storeC));
    map.put(tp(storeB, 44), infoVt(128, 5351890, 0, 0, 969889, storeB));
    map.put(tp(storeB, 45), infoVt(168, 5391638, 0, 0, 385309, storeB));
    map.put(tp(storeA, 32), infoVt(3979827, 4005186, 0, 0, 1120585, storeA));
    map.put(tp(storeA, 0), infoVt(3970739, 3997774, 0, 0, 1226920, storeA));
    map.put(tp(storeC, 82), infoVt(44, 8243155, 0, 0, 969896, storeC));
    map.put(tp(storeC, 21), infoVt(0, 8227759, 0, 0, 385313, storeC));
    map.put(tp(storeA, 25), infoVt(3972689, 4002268, 0, 0, 268715, storeA));
    map.put(tp(storeC, 90), infoVt(0, 8220505, 0, 0, 1120579, storeC));
    map.put(tp(storeA, 58), infoVt(3971168, 3999572, 0, 0, 936733, storeA));
    map.put(tp(storeA, 48), infoVt(3971503, 3999731, 0, 0, 1527710, storeA));
    map.put(tp(storeB, 33), infoVt(353, 5361438, 5.14, 735.75, 53694, storeB));
    map.put(tp(storeA, 52), infoVt(3968092, 3997262, 0, 0, 576128, storeA));

    String out = fmt(map, trigger);
    String[] rows = dataRows(out);
    Assert.assertEquals(rows.length, 16);

    // Top 7 rows are all ~3.97M-lag storeA partitions (sort invariant).
    for (int i = 0; i < 7; i++) {
      Assert.assertTrue(rows[i].contains(storeA + "-"), "row " + i + " not from storeA: " + rows[i]);
    }
    // Trigger marker landed on the right row.
    int triggerRow = rowIndexContaining(rows, "(triggered)");
    Assert.assertTrue(triggerRow >= 0, "trigger row missing:\n" + out);
    Assert.assertTrue(rows[triggerRow].startsWith("  * "), rows[triggerRow]);
    Assert.assertTrue(rows[triggerRow].contains(storeC + "-40"), rows[triggerRow]);
    // 2-decimal rate formatting.
    Assert.assertTrue(out.contains("735.75") && out.contains("5.14"), "rates must round to 2 decimals:\n" + out);
    // versionTopic column populated for each store.
    Assert.assertTrue(out.contains(storeA) && out.contains(storeB) && out.contains(storeC));
  }

  // -------- helpers --------

  /** Returns only the data rows (skipping header line and column-header line). */
  private static String[] dataRows(String out) {
    String[] all = out.split("\n");
    if (all.length < 3) {
      return new String[0];
    }
    String[] data = new String[all.length - 2];
    System.arraycopy(all, 2, data, 0, data.length);
    return data;
  }

  private static int rowIndexContaining(String[] rows, String needle) {
    for (int i = 0; i < rows.length; i++) {
      if (rows[i].contains(needle)) {
        return i;
      }
    }
    return -1;
  }

  private static int occurrences(String haystack, String needle) {
    int count = 0;
    int idx = 0;
    while ((idx = haystack.indexOf(needle, idx)) >= 0) {
      count++;
      idx += needle.length();
    }
    return count;
  }
}
