package com.linkedin.venice.consistency;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ValidatorUtilsTest {
  // TODO add more unit tests for ValidatorUtils
  @Test
  public void testValidatorUtils() {
    List<String> coloLists = new ArrayList<>();

    Collections.addAll(coloLists, "colo1", "colo2");
    List<ImmutablePair<String, String>> coloPairs = ValidatorUtils.getColoPairsToCompare(coloLists);
    Assert.assertEquals(coloPairs.size(), 1);
    Assert.assertEquals(coloPairs.get(0), new ImmutablePair<>("colo1", "colo2"));

    coloLists.clear();
    Collections.addAll(coloLists, "colo1", "colo2", "colo3");
    coloPairs = ValidatorUtils.getColoPairsToCompare(coloLists);
    Assert.assertEquals(coloPairs.size(), 3);
    Assert.assertEquals(coloPairs.get(0), new ImmutablePair<>("colo1", "colo2"));
    Assert.assertEquals(coloPairs.get(2), new ImmutablePair<>("colo2", "colo3"));

    coloLists.clear();
    Collections.addAll(coloLists, "colo1", "colo2", "colo3", "colo4");
    coloPairs = ValidatorUtils.getColoPairsToCompare(coloLists);
    Assert.assertEquals(coloPairs.size(), 6);
    Assert.assertEquals(coloPairs.get(0), new ImmutablePair<>("colo1", "colo2"));
    Assert.assertEquals(coloPairs.get(3), new ImmutablePair<>("colo2", "colo3"));
    Assert.assertEquals(coloPairs.get(5), new ImmutablePair<>("colo3", "colo4"));
  }

  @Test
  public void testValidateValues() {

    String inputKey = "testKey";
    List<String> coloLists = new ArrayList<>();
    String colo1Name = "prod-lva1";
    String colo2Name = "prod-lor1";
    String colo3Name = "prod-ltx1";

    Map<String, List<ValidatorUtils.RecordInfo>> coloOffsetInfos = new HashMap<>();
    Collections.addAll(coloLists, colo1Name, colo2Name, colo3Name);
    List<ImmutablePair<String, String>> coloPairs = ValidatorUtils.getColoPairsToCompare(coloLists);

    Map<String, Map<Integer, List<Long>>> highOffsetWatermarks = new HashMap<>();
    highOffsetWatermarks.put(colo1Name, Collections.singletonMap(0, Arrays.asList(1000L, 1000L, 1000L)));
    highOffsetWatermarks.put(colo2Name, Collections.singletonMap(0, Arrays.asList(1000L, 1000L, 1000L)));
    highOffsetWatermarks.put(colo3Name, Collections.singletonMap(0, Arrays.asList(1000L, 1000L, 1000L)));

    // RecordInfo: valueCheckSum, partition high watermark offset vector, offset vector, offset, timestamp, partition
    List<ValidatorUtils.RecordInfo> recordInfosForColo1 = new ArrayList<>();
    recordInfosForColo1.add(
        new ValidatorUtils.RecordInfo(
            "A",
            colo1Name,
            Arrays.asList(300L, 250L, 220L),
            Arrays.asList(150L, 120L, 50L),
            1L,
            2L,
            0));
    recordInfosForColo1.add(
        new ValidatorUtils.RecordInfo(
            "B",
            colo1Name,
            Arrays.asList(1300L, 1250L, 1220L),
            Arrays.asList(950L, 850L, 50L),
            1L,
            2L,
            0));
    coloOffsetInfos.put(colo1Name, recordInfosForColo1);

    List<ValidatorUtils.RecordInfo> recordInfosForColo2 = new ArrayList<>();
    recordInfosForColo2.add(
        new ValidatorUtils.RecordInfo(
            "B",
            colo2Name,
            Arrays.asList(1000L, 900L, 800L),
            Arrays.asList(950L, 850L, 750L),
            1L,
            2L,
            0));
    coloOffsetInfos.put(colo2Name, recordInfosForColo2);

    List<ValidatorUtils.RecordInfo> recordInfosForColo3 = new ArrayList<>();
    recordInfosForColo3.add(
        new ValidatorUtils.RecordInfo(
            "C",
            colo3Name,
            Arrays.asList(200L, 150L, 100L),
            Arrays.asList(190L, 140L, 70L),
            1L,
            2L,
            0));
    recordInfosForColo3.add(
        new ValidatorUtils.RecordInfo(
            "B",
            colo3Name,
            Arrays.asList(1050L, 950L, 850L),
            Arrays.asList(950L, 859L, 750L),
            1L,
            2L,
            0));
    coloOffsetInfos.put(colo3Name, recordInfosForColo3);

    /**
     * For this case, two regions: prod-lva1 and prod-ltx1 will have transient inconsistency between value A and C.
     * Region prod-lva1's partition high watermark is ahead of record C 's offset vector. Similarly, region prod-ltx1's
     *  partition high watermark is ahead of record B's offset vector. The lily pad aims at finding this kind of
     * transient inconsistency.
     *
     * For now, record of value A and record of value C are not comparable to prod-lor1, prod-lor1's
     * record of value B's partition high watermark and record offset vector show it is completely ahead.
     */
    ValidatorUtils.ValidationResult result =
        ValidatorUtils.completeValidateValues(inputKey, coloOffsetInfos, coloPairs, highOffsetWatermarks);
    Assert.assertTrue(result.isMismatch);
    Assert.assertTrue(result.inconsistentRecordsPairs.containsKey(colo1Name));
    Assert.assertEquals(
        result.inconsistentRecordsPairs.get(colo1Name).valueCheckSum,
        recordInfosForColo1.get(0).valueCheckSum);
    Assert.assertTrue(result.inconsistentRecordsPairs.containsKey(colo3Name));
    Assert.assertEquals(
        result.inconsistentRecordsPairs.get(colo3Name).valueCheckSum,
        recordInfosForColo3.get(0).valueCheckSum);
  }

  @Test
  public void testValidateValuesTwoRegions() {
    String inputKey = "testKey";
    List<String> coloLists = new ArrayList<>();
    String colo1Name = "prod-lva1";
    String colo2Name = "prod-lor1";

    Map<String, List<ValidatorUtils.RecordInfo>> coloOffsetInfos = new HashMap<>();
    Collections.addAll(coloLists, colo1Name, colo2Name);
    List<ImmutablePair<String, String>> coloPairs = ValidatorUtils.getColoPairsToCompare(coloLists);

    Map<String, Map<Integer, List<Long>>> highOffsetWatermarks = new HashMap<>();
    highOffsetWatermarks.put(colo1Name, Collections.singletonMap(0, Arrays.asList(1000L, 1000L, 1000L)));
    highOffsetWatermarks.put(colo2Name, Collections.singletonMap(0, Arrays.asList(1000L, 1000L, 1000L)));

    // RecordInfo: valueCheckSum, partition high watermark offset vector, offset vector, offset, timestamp, partition
    List<ValidatorUtils.RecordInfo> recordInfosForColo1 = new ArrayList<>();
    recordInfosForColo1.add(
        new ValidatorUtils.RecordInfo(
            "A",
            colo1Name,
            Arrays.asList(100L, 200L, 300L),
            Arrays.asList(100L, 200L, 290L),
            1L,
            11L,
            0));
    recordInfosForColo1.add(
        new ValidatorUtils.RecordInfo(
            "D",
            colo1Name,
            Arrays.asList(400L, 230L, 300L),
            Arrays.asList(370L, 220L, 300L),
            2L,
            12L,
            0));
    recordInfosForColo1.add(
        new ValidatorUtils.RecordInfo(
            "E",
            colo1Name,
            Arrays.asList(600L, 330L, 400L),
            Arrays.asList(470L, 320L, 400L),
            3L,
            13L,
            0));
    coloOffsetInfos.put(colo1Name, recordInfosForColo1);

    List<ValidatorUtils.RecordInfo> recordInfosForColo2 = new ArrayList<>();
    recordInfosForColo2.add(
        new ValidatorUtils.RecordInfo(
            "B",
            colo2Name,
            Arrays.asList(250L, 195L, 310L),
            Arrays.asList(120L, 195L, 310L),
            1L,
            11L,
            0));
    recordInfosForColo2.add(
        new ValidatorUtils.RecordInfo(
            "C",
            colo2Name,
            Arrays.asList(300L, 250L, 320L),
            Arrays.asList(120L, 195L, 320L),
            2L,
            12L,
            0));
    recordInfosForColo2.add(
        new ValidatorUtils.RecordInfo(
            "E",
            colo2Name,
            Arrays.asList(700L, 325L, 400L),
            Arrays.asList(470L, 270L, 399L),
            3L,
            13L,
            0));
    recordInfosForColo2.add(
        new ValidatorUtils.RecordInfo(
            "F",
            colo2Name,
            Arrays.asList(750L, 328L, 420L),
            Arrays.asList(470L, 328L, 399L),
            4L,
            14L,
            0));
    coloOffsetInfos.put(colo2Name, recordInfosForColo2);

    /**
     * For this case, two regions: prod-lva1 and prod-lor1 will have transient inconsistency between value E and F.
     * Region prod-lva1's partition high watermark is ahead of record F 's offset vector. Similarly, region prod-lor1's
     * partition high watermark is ahead of record E's offset vector. The lily pad aims at leaping with such 7 pairs to compare:
     *           prod-lva1                     prod-lor1
     *              A                               B                        leap prod-lor1
     *              A                               C                        leap prod-lva1
     *              D                               C                        leap prod-lor1
     *              D                               E                        leap prod-lva1
     *              E                               E                        leap prod-lor1
     *              E                               F                        inconsistency found.
     */
    ValidatorUtils.ValidationResult result =
        ValidatorUtils.completeValidateValues(inputKey, coloOffsetInfos, coloPairs, highOffsetWatermarks);
    Assert.assertTrue(result.isMismatch);
    Assert.assertTrue(result.numOfComparisons.equals(6L));
    Assert.assertTrue(result.inconsistentRecordsPairs.containsKey(colo1Name));
    Assert.assertTrue(result.inconsistentRecordsPairs.containsKey(colo2Name));
    Assert.assertEquals(
        result.inconsistentRecordsPairs.get(colo1Name).valueCheckSum,
        recordInfosForColo1.get(2).valueCheckSum);
    Assert.assertEquals(
        result.inconsistentRecordsPairs.get(colo2Name).valueCheckSum,
        recordInfosForColo2.get(3).valueCheckSum);
  }

  @Test
  public void testForDelete() {

    String inputKey = "testKey";
    List<String> coloLists = new ArrayList<>();
    String colo1Name = "prod-lva1";
    String colo2Name = "prod-lor1";
    Map<String, List<ValidatorUtils.RecordInfo>> coloOffsetInfos = new HashMap<>();
    Collections.addAll(coloLists, colo1Name, colo2Name);
    List<ImmutablePair<String, String>> coloPairs = ValidatorUtils.getColoPairsToCompare(coloLists);
    Map<String, Map<Integer, List<Long>>> highOffsetWatermarks = new HashMap<>();
    highOffsetWatermarks.put(colo1Name, Collections.singletonMap(0, Arrays.asList(100L, 110L, 120L)));
    highOffsetWatermarks.put(colo2Name, Collections.singletonMap(0, Arrays.asList(800L, 800L, 800L)));

    ValidatorUtils.ValidationResult result =
        ValidatorUtils.completeValidateValues(inputKey, coloOffsetInfos, coloPairs, highOffsetWatermarks);
    Assert.assertFalse(result.isMismatch);
    Assert.assertTrue(result.numOfComparisons.equals(0L));

    /**
     * For this case, region prod-lva1 does not have any record for this key, while other region prod-lor1 have two records
     * for this key. Our algorithm will create a fake record for region prod-lva1 as delete with the lowest of record offset
     * vector [0, 0, 0] and high partition watermark as [1000, 1000, 1000]. So that lily pad algorithm will detect this kind
     * of inconsistency, as prod-lva1 should have seen record of value B.
     */
    // RecordInfo: valueCheckSum, partition high watermark offset vector, offset vector, offset, timestamp, partition
    List<ValidatorUtils.RecordInfo> recordInfosForColo2 = new ArrayList<>();
    recordInfosForColo2.add(
        new ValidatorUtils.RecordInfo(
            "B",
            colo2Name,
            Arrays.asList(250L, 195L, 310L),
            Arrays.asList(120L, 195L, 310L),
            1L,
            11L,
            0));
    recordInfosForColo2.add(
        new ValidatorUtils.RecordInfo(
            "C",
            colo2Name,
            Arrays.asList(300L, 250L, 320L),
            Arrays.asList(120L, 195L, 320L),
            2L,
            12L,
            0));
    coloOffsetInfos.put(colo2Name, recordInfosForColo2);

    /**
     * For this case, region prod-lva1 does not have any record for this key, while other region prod-lor1 have two records
     * for this key. Our algorithm will create a fake record for region prod-lva1 as delete with the lowest of record offset
     * vector [0, 0, 0] and high partition watermark as [100, 110, 120]. But lily pad algorithm will treat this as not
     * comparable, as prod-lva1 high partition watermark is not ahead of prod-lor1 offset vector.
     */
    result = ValidatorUtils.completeValidateValues(inputKey, coloOffsetInfos, coloPairs, highOffsetWatermarks);
    Assert.assertFalse(result.isMismatch);
    Assert.assertTrue(result.numOfComparisons.equals(1L));

    /**
     * For this case, prod-lva1's high partition watermark changes to [1000, 1000, 1000]. So that lily pad algorithm will
     * detect this kind of inconsistency, as prod-lva1 should have seen record of value B.
     */
    coloOffsetInfos.remove(colo1Name);
    highOffsetWatermarks.put(colo1Name, Collections.singletonMap(0, Arrays.asList(1000L, 1000L, 1000L)));
    result = ValidatorUtils.completeValidateValues(inputKey, coloOffsetInfos, coloPairs, highOffsetWatermarks);
    Assert.assertTrue(result.isMismatch);
    Assert.assertTrue(result.numOfComparisons.equals(1L));
  }

}
