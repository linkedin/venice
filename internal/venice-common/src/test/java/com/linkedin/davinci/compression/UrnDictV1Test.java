package com.linkedin.davinci.compression;

import static org.testng.Assert.assertThrows;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit tests for UrnDictV1.
 */
public class UrnDictV1Test {
  @Test
  public void testTrainDict() {
    List<String> urns = Arrays.asList("urn:li:user:123", "urn:li:user:456", "urn:li:group:789", "urn:li:company:101");

    UrnDictV1 dict = UrnDictV1.trainDict(urns, 3);
    Map<String, Integer> urnToIdMap = dict.getUrnToIdMap();

    Assert.assertEquals(urnToIdMap.size(), 3);
    Assert.assertTrue(urnToIdMap.containsKey("user"));
    Assert.assertTrue(urnToIdMap.containsKey("group"));
    Assert.assertTrue(urnToIdMap.containsKey("company"));
  }

  @Test
  public void testTrainDictWithEmptyUrns() {
    UrnDictV1 dict = UrnDictV1.trainDict(Collections.emptyList(), 10);
    Assert.assertTrue(dict.getUrnToIdMap().isEmpty());
  }

  @Test
  public void testTrainDictWithInvalidUrns() {
    List<String> urns = Arrays.asList("invalid-urn", "urn:li:user:123", "http://example.com");
    UrnDictV1 dict = UrnDictV1.trainDict(urns, 10);
    Map<String, Integer> urnToIdMap = dict.getUrnToIdMap();
    Assert.assertEquals(urnToIdMap.size(), 1);
    Assert.assertTrue(urnToIdMap.containsKey("user"));
  }

  @Test(expectedExceptions = com.linkedin.venice.exceptions.VeniceException.class)
  public void testTrainDictWithInvalidDictSize() {
    UrnDictV1.trainDict(Collections.singletonList("urn:li:user:123"), 0);
  }

  @Test
  public void testTrainDictWithLargerDictSize() {
    List<String> urns = Arrays.asList("urn:li:user:123", "urn:li:group:456");
    UrnDictV1 dict = UrnDictV1.trainDict(urns, 10);
    Assert.assertEquals(dict.getUrnToIdMap().size(), 2);
  }

  @Test
  public void testTrainDictWithDuplicateUrns() {
    List<String> urns = Arrays.asList(
        "urn:li:user:123",
        "urn:li:user:456",
        "urn:li:group:789",
        "urn:li:user:123",
        "urn:li:company:101",
        "urn:li:group:789",
        "urn:li:user:abc");

    UrnDictV1 dict = UrnDictV1.trainDict(urns, 3);
    Map<String, Integer> urnToIdMap = dict.getUrnToIdMap();

    Assert.assertEquals(urnToIdMap.size(), 3);
    Assert.assertTrue(urnToIdMap.containsKey("user"));
    Assert.assertTrue(urnToIdMap.containsKey("group"));
    Assert.assertTrue(urnToIdMap.containsKey("company"));

    Assert.assertEquals(urnToIdMap.get("user").intValue(), 0);
    Assert.assertEquals(urnToIdMap.get("group").intValue(), 1);
    Assert.assertEquals(urnToIdMap.get("company").intValue(), 2);
  }

  @Test
  public void testEncodeDecodeUrn() {
    List<String> urns = Arrays.asList("urn:li:user:123", "urn:li:group:456");
    UrnDictV1 dict = UrnDictV1.trainDict(urns, 2);

    String validUrn = "urn:li:user:789";
    UrnDictV1.EncodedUrn encoded = dict.encodeUrn(validUrn);
    Assert.assertEquals(encoded.urnTypeId, 0);
    Assert.assertEquals(encoded.urnRemainder, "789");

    String decodedUrn = dict.decodeUrn(encoded);
    Assert.assertEquals(decodedUrn, validUrn);

    // Test unknown URN type
    String unknownUrn = "urn:li:company:101";
    UrnDictV1.EncodedUrn unknownEncoded = dict.encodeUrn(unknownUrn);
    Assert.assertEquals(unknownEncoded.urnTypeId, UrnDictV1.UNKNOWN_URN_TYPE_ID);
    Assert.assertEquals(unknownEncoded.urnRemainder, unknownUrn);
    Assert.assertEquals(dict.decodeUrn(unknownEncoded), unknownUrn);

    // Test invalid URN format
    String invalidUrn = "not-a-urn";
    UrnDictV1.EncodedUrn invalidEncoded = dict.encodeUrn(invalidUrn);
    Assert.assertEquals(invalidEncoded.urnTypeId, UrnDictV1.UNKNOWN_URN_TYPE_ID);
    Assert.assertEquals(dict.decodeUrn(invalidEncoded), invalidUrn);
  }

  @Test(expectedExceptions = com.linkedin.venice.exceptions.VeniceException.class)
  public void testDecodeWithInvalidTypeId() {
    UrnDictV1 dict = UrnDictV1.trainDict(Collections.singletonList("urn:li:user:123"), 1);
    UrnDictV1.EncodedUrn encoded = new UrnDictV1.EncodedUrn(100, "123");
    dict.decodeUrn(encoded);
  }

  @Test
  public void testDynamicDictGrowthInEncode() {
    // Initial dict has 'user' (id 0) and 'group' (id 1). Max size is 3.
    UrnDictV1 dict = UrnDictV1.trainDict(Arrays.asList("urn:li:user:1", "urn:li:group:2"), 3);
    Assert.assertEquals(dict.getUrnToIdMap().size(), 2);

    // 1. Encode a new URN type 'company'. Should be added to dict with id 2.
    String newUrn = "urn:li:company:3";
    UrnDictV1.EncodedUrn encodedNew = dict.encodeUrn(newUrn);
    Assert.assertEquals(encodedNew.urnTypeId, 2, "A new type should be added to the dictionary");
    Assert.assertEquals(dict.getUrnToIdMap().size(), 3, "Dictionary size should increase");
    Assert.assertEquals(dict.decodeUrn(encodedNew), newUrn, "The new URN should decode correctly");

    // 2. Dict is now full. Try to encode another new URN type 'email'. Should be rejected.
    String anotherNewUrn = "urn:li:email:4";
    UrnDictV1.EncodedUrn encodedAnother = dict.encodeUrn(anotherNewUrn);
    Assert.assertEquals(encodedAnother.urnTypeId, UrnDictV1.UNKNOWN_URN_TYPE_ID, "Should not add to a full dictionary");
    Assert.assertEquals(dict.getUrnToIdMap().size(), 3, "Dictionary size should not change");
    Assert.assertEquals(dict.decodeUrn(encodedAnother), anotherNewUrn, "Unknown URN should decode to itself");

    // 3. Encode an existing URN type 'user'. Should still work.
    String existingUrn = "urn:li:user:5";
    UrnDictV1.EncodedUrn encodedExisting = dict.encodeUrn(existingUrn);
    Assert.assertEquals(encodedExisting.urnTypeId, 0, "Existing URN should be encoded correctly");
  }

  @Test
  public void testConcurrentEncode() throws InterruptedException {
    final int maxDictSize = 50;
    final int initialSize = 10;
    final int numThreads = 20;
    final int numUrnsPerThread = 10; // Total attempts = 200

    List<String> initialUrns = new java.util.ArrayList<>();
    for (int i = 0; i < initialSize; i++) {
      initialUrns.add("urn:li:initial_type_" + i + ":1");
    }
    final UrnDictV1 dict = UrnDictV1.trainDict(initialUrns, maxDictSize);

    final ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    final ConcurrentLinkedQueue<UrnDictV1.EncodedUrn> results = new ConcurrentLinkedQueue<>();

    for (int i = 0; i < numThreads * numUrnsPerThread; i++) {
      final String urn = "urn:li:new_type_" + i + ":1";
      executor.submit(() -> results.add(dict.encodeUrn(urn)));
    }

    executor.shutdown();
    Assert.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));

    Assert.assertEquals(dict.getUrnToIdMap().size(), maxDictSize, "Dictionary should grow to its max size");

    long successfulEncodes = results.stream().filter(e -> e.urnTypeId != UrnDictV1.UNKNOWN_URN_TYPE_ID).count();
    long unknownEncodes = results.stream().filter(e -> e.urnTypeId == UrnDictV1.UNKNOWN_URN_TYPE_ID).count();

    Assert.assertEquals(successfulEncodes, maxDictSize - initialSize, "Should successfully encode until dict is full");
    Assert.assertEquals(
        unknownEncodes,
        (numThreads * numUrnsPerThread) - (maxDictSize - initialSize),
        "Should reject new URNs after dict is full");

    Set<Integer> assignedIds = dict.getUrnToIdMap().values().stream().collect(Collectors.toSet());
    Assert.assertEquals(assignedIds.size(), maxDictSize, "All assigned IDs must be unique");
  }

  @Test
  public void testSerializeAndLoadDict() {
    List<String> urns = Arrays.asList("urn:li:user:1", "urn:li:group:2", "urn:li:company:3");
    UrnDictV1 originalDict = UrnDictV1.trainDict(urns, 5);

    // Add one more entry dynamically to test state after training
    originalDict.encodeUrn("urn:li:job:4");

    byte[] serializedDict = originalDict.serializeDict();
    UrnDictV1 loadedDict = UrnDictV1.loadDict(serializedDict, 5);

    Assert.assertEquals(
        loadedDict.getUrnToIdMap(),
        originalDict.getUrnToIdMap(),
        "Loaded dictionary map should be identical to the original");

    // Verify that the loaded dictionary functions correctly
    String existingUrn = "urn:li:user:123";
    UrnDictV1.EncodedUrn originalEncoded = originalDict.encodeUrn(existingUrn);
    UrnDictV1.EncodedUrn loadedEncoded = loadedDict.encodeUrn(existingUrn);
    Assert.assertEquals(loadedEncoded.urnTypeId, originalEncoded.urnTypeId);
  }

  @Test
  public void testLoadDictWithNullOrEmpty() {
    assertThrows(
        com.linkedin.venice.exceptions.VeniceException.class,
        () -> UrnDictV1.loadDict((Map<String, Integer>) null, 10));
    assertThrows(com.linkedin.venice.exceptions.VeniceException.class, () -> UrnDictV1.loadDict((byte[]) null, 10));
    assertThrows(com.linkedin.venice.exceptions.VeniceException.class, () -> UrnDictV1.loadDict(new byte[0], 10));
  }

  @Test
  public void testLoadDictWithInvalidSize() {
    Map<String, Integer> dictMap = Collections.singletonMap("user", 0);
    byte[] serialized = UrnDictV1.loadDict(dictMap, 1).serializeDict();

    assertThrows(VeniceException.class, () -> UrnDictV1.loadDict(dictMap, 0));
    assertThrows(VeniceException.class, () -> UrnDictV1.loadDict(dictMap, -1));
    assertThrows(VeniceException.class, () -> UrnDictV1.loadDict(serialized, 0));
    assertThrows(VeniceException.class, () -> UrnDictV1.loadDict(serialized, -1));
  }

  @Test
  public void testLoadDictWithGap() {
    // Create a dictionary with non-contiguous IDs: 0 and 2.
    Map<String, Integer> mapWithGap = new java.util.HashMap<>();
    mapWithGap.put("user", 0);
    mapWithGap.put("company", 2);
    assertThrows(VeniceException.class, () -> UrnDictV1.loadDict(mapWithGap, 3));
  }

  @Test
  public void testEncodeWithDictUpdateDisabled() {
    UrnDictV1 dict = UrnDictV1.trainDict(Collections.singletonList("urn:li:user:1"), 2);
    Assert.assertEquals(dict.getUrnToIdMap().size(), 1);

    // Try to encode a new URN type with updates disabled.
    String newUrn = "urn:li:company:2";
    UrnDictV1.EncodedUrn encoded = dict.encodeUrn(newUrn, false);

    // The URN should be unknown, and the dictionary size should not change.
    Assert.assertEquals(encoded.urnTypeId, UrnDictV1.UNKNOWN_URN_TYPE_ID);
    Assert.assertEquals(dict.getUrnToIdMap().size(), 1, "Dictionary size should not change when updates are disabled");

    // Encoding an existing URN should still work.
    String existingUrn = "urn:li:user:3";
    UrnDictV1.EncodedUrn existingEncoded = dict.encodeUrn(existingUrn, false);
    Assert.assertNotEquals(existingEncoded.urnTypeId, UrnDictV1.UNKNOWN_URN_TYPE_ID);
  }

}
