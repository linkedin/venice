package com.linkedin.venice.consistency;

import static java.util.stream.Collectors.*;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.IndexedRecord;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public final class ValidatorUtils {
  private ValidatorUtils() {

  }

  private static final Logger log = LogManager.getLogger(ValidatorUtils.class);
  public static final Splitter COMMA_SPLITTER = Splitter.on(',').omitEmptyStrings();

  public static final String DELETE_CHECKSUM_VALUE = "0";

  /**
   * Taken and modified from org.apache.avro.generic.GenericData
   * This sorts the map fields in the record, so that string representation always be in the same order
   * @param datum
   * @return string representation of Avro record
   */
  @VisibleForTesting
  public static String getSortedDatum(Object datum) {
    StringBuilder buffer = new StringBuilder();
    toString(datum, buffer);
    return buffer.toString();
  }

  // Taken and modified from org.apache.avro.generic.GenericData
  protected static void toString(Object datum, StringBuilder buffer) {
    GenericData genericData = GenericData.get();
    if (datum instanceof IndexedRecord) {
      buffer.append("{");
      int count = 0;
      Schema schema = ((GenericContainer) datum).getSchema();
      for (Schema.Field f: schema.getFields()) {
        toString(f.name(), buffer);
        buffer.append(": ");
        toString(genericData.getField(datum, f.name(), f.pos()), buffer);
        if (++count < schema.getFields().size()) {
          buffer.append(", ");
        }
      }
      buffer.append("}");
    } else if (datum instanceof Collection) {
      Collection<?> array = (Collection<?>) datum;
      buffer.append("[");
      long last = array.size() - 1;
      int i = 0;
      for (Object element: array) {
        toString(element, buffer);
        if (i++ < last) {
          buffer.append(", ");
        }
      }
      buffer.append("]");
    } else if (datum instanceof Map) {
      // Sorting of map elements by key
      // avro map keys should always be Utf8
      buffer.append("{");
      Map<String, Object> map = (Map<String, Object>) datum;
      String sortedMap = map.entrySet()
          .stream()
          .sorted(Map.Entry.comparingByKey())
          .map(e -> getSortedDatum(e.getKey()) + ": " + getSortedDatum(e.getValue()))
          .collect(joining(", "));
      buffer.append(new StringBuffer(sortedMap));
      buffer.append("}");
    } else if (datum instanceof CharSequence || datum instanceof GenericEnumSymbol) {
      buffer.append("\"");
      writeEscapedString(datum.toString(), buffer);
      buffer.append("\"");
    } else if (datum instanceof ByteBuffer) {
      buffer.append("{\"bytes\": \"");
      ByteBuffer bytes = (ByteBuffer) datum;
      for (int i = bytes.position(); i < bytes.limit(); i++) {
        buffer.append((char) bytes.get(i));
      }
      buffer.append("\"}");
    } else if (((datum instanceof Float) && // quote Nan & Infinity
        (((Float) datum).isInfinite() || ((Float) datum).isNaN()))
        || ((datum instanceof Double) && (((Double) datum).isInfinite() || ((Double) datum).isNaN()))) {
      buffer.append("\"");
      buffer.append(datum);
      buffer.append("\"");
    } else {
      buffer.append(datum);
    }
  }

  // Taken from org.apache.avro.generic.GenericData
  @SuppressWarnings("FallThroughCheck")
  private static void writeEscapedString(String string, StringBuilder builder) {
    for (int i = 0; i < string.length(); i++) {
      char ch = string.charAt(i);
      switch (ch) {
        case '"':
          builder.append("\\\"");
          break;
        case '\\':
          builder.append("\\\\");
          break;
        case '\b':
          builder.append("\\b");
          break;
        case '\f':
          builder.append("\\f");
          break;
        case '\n':
          builder.append("\\n");
          break;
        case '\r':
          builder.append("\\r");
          break;
        case '\t':
          builder.append("\\t");
          break;
        default:
          // Reference: http://www.unicode.org/versions/Unicode5.1.0/
          if ((ch >= '\u0000' && ch <= '\u001F') || (ch >= '\u007F' && ch <= '\u009F')
              || (ch >= '\u2000' && ch <= '\u20FF')) {
            String hex = Integer.toHexString(ch);
            builder.append("\\u");
            for (int j = 0; j < 4 - hex.length(); j++) {
              builder.append('0');
            }
            builder.append(hex.toUpperCase());
          } else {
            builder.append(ch);
          }
          break; // TODO: removed if has bug
      }
    }
  }

  /***
   * Every time we can only compare two colos, this method will prepare all possible pairs to be compared based on input
   * colo list. Reference for why we need to exhaustively compare all the colo pairs:
   * https://rb.corp.linkedin.com/r/2853363/#comment8898755
   * @param colos The colos we have already have its ETL data ready in HDFS, we will to do consistency check on.
   * @return The list of colo pairs to be applied consistency check on.
   */
  public static List<ImmutablePair<String, String>> getColoPairsToCompare(List<String> colos) {
    List<ImmutablePair<String, String>> coloPairs = new ArrayList<>();
    int coloCount = colos.size();
    for (int i = 0; i < coloCount; i++) {
      for (int j = i + 1; j < coloCount; j++) {
        coloPairs.add(new ImmutablePair<>(colos.get(i), colos.get(j)));
      }
    }
    return coloPairs;
  }

  /***
   * Implementation for lily pad algorithm to check every historical values for a key. It relies on temporary partition
   * partition high watermark for very historical value in every region. It generally leaps from non-decidable values pairs
   * to find all comparable pairs and examine all of them. It will drop one historical value each round, then we can have
   * linear time complexity to go through all historical values.
   * @param inputKey the key for informative logging purpose.
   * @param coloOffsetInfos infos for all historic values, including region, offset vector and high partition watermark.
   * @param coloPairs pairs of region to be validated.
   * @param highOffsetWatermarks partition high offset watermark for different region to compare.
   * @return validation result with boolean flag, error message and validated record counts.
   */
  public static ValidationResult completeValidateValues(
      String inputKey,
      Map<String, List<RecordInfo>> coloOffsetInfos,
      List<ImmutablePair<String, String>> coloPairs,
      Map<String, Map<Integer, List<Long>>> highOffsetWatermarks) {
    long numOfComparisons = 0;
    Map<String, RecordInfo> inconsistentRecordsPairs = new HashMap<>();

    if (coloOffsetInfos.size() == 0) {
      String exceptionMsg =
          "Not enough colos to be compared, we should have no less than 1 colo with record to compare. ";
      return new ValidationResult(false, exceptionMsg, 0L, inconsistentRecordsPairs);
    }

    for (ImmutablePair<String, String> coloPair: coloPairs) {
      String colo1 = coloPair.left;
      String colo2 = coloPair.right;
      int colo1Index = 0;
      int colo2Index = 0;

      if (!coloOffsetInfos.containsKey(colo1) && !coloOffsetInfos.containsKey(colo2)) {
        log.debug("Missing offset information for those two colos: " + coloPairs + ", skip comparison.");
        continue;
      }

      if (!coloOffsetInfos.containsKey(colo1) || !coloOffsetInfos.containsKey(colo2)) {

        String coloWithoutRecord = colo1;
        String coloWithRecords = colo2;
        if (!coloOffsetInfos.containsKey(colo2)) {
          coloWithoutRecord = colo2;
          coloWithRecords = colo1;
        }

        int partitionId = coloOffsetInfos.get(coloWithRecords).get(0).partition;
        List<Long> partitionHighOffsetWatermark = highOffsetWatermarks.get(coloWithoutRecord).get(partitionId);

        // Creating fake record for the colo. Using record offset vector of 0 to guarantee other colo always has
        // partition
        // high watermark higher than this colo, maximizing the chance to be comparable.
        List<Long> fakeRecordOffsetVector =
            new ArrayList<>(Collections.nCopies(partitionHighOffsetWatermark.size(), 0L));
        RecordInfo fakeNewRecord = new RecordInfo(
            DELETE_CHECKSUM_VALUE,
            coloWithoutRecord,
            partitionHighOffsetWatermark,
            fakeRecordOffsetVector,
            1L,
            0L,
            partitionId);
        coloOffsetInfos.put(coloWithoutRecord, Arrays.asList(fakeNewRecord));
      }

      while (colo1Index < coloOffsetInfos.get(colo1).size() && colo2Index < coloOffsetInfos.get(colo2).size()) {
        RecordInfo colo1OffsetInfo = coloOffsetInfos.get(colo1).get(colo1Index);
        RecordInfo colo2OffsetInfo = coloOffsetInfos.get(colo2).get(colo2Index);
        String comparisonDetail = "Comparing for two colos: " + colo1 + ":" + colo2
            + prepareComparisonDetail(colo1OffsetInfo, colo2OffsetInfo) + ",\n with indexes: " + colo1Index + ":"
            + colo2Index + ",\n with records #: " + coloOffsetInfos.get(colo1).size() + ":"
            + coloOffsetInfos.get(colo2).size();

        boolean hasColo1OffsetAdvanced =
            DiffValidationUtils.hasOffsetAdvanced(colo2OffsetInfo.recordOffsets, colo1OffsetInfo.currentHighWatermarks);
        boolean hasColo2OffsetAdvanced =
            DiffValidationUtils.hasOffsetAdvanced(colo1OffsetInfo.recordOffsets, colo2OffsetInfo.currentHighWatermarks);
        Boolean isMismatch = DiffValidationUtils.doRecordsDiverge(
            colo1OffsetInfo.valueCheckSum,
            colo2OffsetInfo.valueCheckSum,
            colo1OffsetInfo.currentHighWatermarks,
            colo2OffsetInfo.currentHighWatermarks,
            colo1OffsetInfo.recordOffsets,
            colo2OffsetInfo.recordOffsets);
        numOfComparisons++;

        if (isMismatch) {
          // Add previous and next record detail for better debugging information.
          if (colo1Index < coloOffsetInfos.get(colo1).size() - 1
              && colo2Index < coloOffsetInfos.get(colo2).size() - 1) {
            RecordInfo colo1NextRecordInfo = coloOffsetInfos.get(colo1).get(colo1Index + 1);
            RecordInfo colo2NextRecordInfo = coloOffsetInfos.get(colo2).get(colo2Index + 1);
            String nextRecordDetail =
                "\n Next record detail: " + prepareComparisonDetail(colo1NextRecordInfo, colo2NextRecordInfo);
            comparisonDetail = comparisonDetail + nextRecordDetail;
          }

          if (colo1Index > 0 && colo2Index > 0) {
            RecordInfo colo1PreviousRecordInfo = coloOffsetInfos.get(colo1).get(colo1Index - 1);
            RecordInfo colo2PreviousRecordInfo = coloOffsetInfos.get(colo2).get(colo2Index - 1);
            String previousRecordDetail = "\n Previous record detail: "
                + prepareComparisonDetail(colo1PreviousRecordInfo, colo2PreviousRecordInfo);
            comparisonDetail = comparisonDetail + previousRecordDetail;
          }

          String errorMessage = String.format("Mismatch for key: %s \n when %s", inputKey, comparisonDetail);
          log.debug(errorMessage);
          inconsistentRecordsPairs.put(colo1, colo1OffsetInfo);
          inconsistentRecordsPairs.put(colo2, colo2OffsetInfo);
          return new ValidationResult(true, errorMessage, numOfComparisons, inconsistentRecordsPairs);
        } else {
          if (colo2OffsetInfo.valueCheckSum.equals(colo1OffsetInfo.valueCheckSum)
              || (!hasColo1OffsetAdvanced && !hasColo2OffsetAdvanced)) {
            // Randomly choose one colo to leap, here we always leap colo1 if possible.
            if (colo1Index + 1 < coloOffsetInfos.get(colo1).size()) {
              log.debug("Leap one colo randomly: " + colo1 + " from index: " + colo1Index);
              colo1Index++;
            } else {
              log.debug("Leap one colo randomly: " + colo2 + " from index: " + colo2Index);
              colo2Index++;
            }
          } else {
            if (hasColo1OffsetAdvanced) {
              log.debug("Leap one colo: " + colo2 + " from index: " + colo2Index);
              colo2Index++;
            } else if (hasColo2OffsetAdvanced) {
              log.debug("Leap one colo: " + colo1 + " from index: " + colo1Index);
              colo1Index++;
            }
          }
        }
      }
    }

    // Validate as successful, update counters and log.
    String successfulMessage = String.format("Successful match for key: %s", inputKey);
    return new ValidationResult(false, successfulMessage, numOfComparisons, inconsistentRecordsPairs);
  }

  private static String prepareComparisonDetail(RecordInfo colo1OffsetInfo, RecordInfo colo2OffsetInfo) {
    return ",\n with checksums: " + colo1OffsetInfo.valueCheckSum + ":" + colo2OffsetInfo.valueCheckSum
        + ",\n with record offset vector: " + colo1OffsetInfo.recordOffsets + ":" + colo2OffsetInfo.recordOffsets
        + ",\n with current partition high watermark offset vector: " + colo1OffsetInfo.currentHighWatermarks + ":"
        + colo2OffsetInfo.currentHighWatermarks + ",\n with timestamp: " + colo1OffsetInfo.timestamp + ":"
        + colo2OffsetInfo.timestamp + ",\n with offset: " + colo1OffsetInfo.versionTopicOffset + ":"
        + colo2OffsetInfo.versionTopicOffset + ",\n with partition: " + colo1OffsetInfo.partition + ":"
        + colo2OffsetInfo.partition;
  }

  public static class ValidationResult {
    Boolean isMismatch;
    String errorMsg;
    Long numOfComparisons;
    Map<String, RecordInfo> inconsistentRecordsPairs;

    public ValidationResult(
        Boolean isMismatch,
        String errorMsg,
        Long numOfComparisons,
        Map<String, RecordInfo> inconsistentRecordsPairs) {
      this.isMismatch = isMismatch;
      this.errorMsg = errorMsg;
      this.numOfComparisons = numOfComparisons;
      this.inconsistentRecordsPairs = inconsistentRecordsPairs;
    }
  }

  public static class RecordInfo {
    String valueCheckSum;
    String coloRegion;
    List<Long> currentHighWatermarks;
    List<Long> recordOffsets;
    Long versionTopicOffset;
    Long timestamp;
    int partition;

    public RecordInfo(
        String valueCheckSum,
        String coloRegion,
        List<Long> currentHighWatermarks,
        List<Long> recordOffsets,
        Long versionTopicOffset,
        Long timestamp,
        int partition) {
      this.valueCheckSum = valueCheckSum;
      this.coloRegion = coloRegion;
      this.currentHighWatermarks = currentHighWatermarks;
      this.recordOffsets = recordOffsets;
      this.versionTopicOffset = versionTopicOffset;
      this.timestamp = timestamp;
      this.partition = partition;
    }
  }

}
