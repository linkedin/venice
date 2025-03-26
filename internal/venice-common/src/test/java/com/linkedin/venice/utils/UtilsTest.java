package com.linkedin.venice.utils;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import static org.testng.Assert.fail;

import com.linkedin.venice.exceptions.ErrorType;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceHttpException;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.avro.Schema;
import org.apache.http.HttpStatus;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import org.testng.collections.Lists;


/**
 * Test cases for Venice {@link Utils}
 */
public class UtilsTest {
  final static String STORE_NAME = "TestStore";

  @Test
  public void testGetHelixNodeIdentifier() {
    int port = 1234;
    assertEquals(
        Utils.getHelixNodeIdentifier(Utils.getHostName(), 1234),
        Utils.getHostName() + "_" + port,
        "Identifier is not the valid format required by Helix.");

    String fixedHostname = "my_host";
    assertEquals(
        Utils.getHelixNodeIdentifier(fixedHostname, 1234),
        fixedHostname + "_" + port,
        "Identifier is not the valid format required by Helix.");
    long memSize = Utils.getOSMemorySize();
    assertTrue(memSize > 0);
  }

  @Test
  public void testParseHostAndPortFromNodeIdentifier() {
    int port = 1234;
    String host = Utils.getHostName();
    String identifier = Utils.getHelixNodeIdentifier(host, 1234);
    assertEquals(Utils.parseHostFromHelixNodeIdentifier(identifier), host);
    assertEquals(Utils.parsePortFromHelixNodeIdentifier(identifier), port);

    identifier = "my_host_" + port;
    assertEquals(Utils.parseHostFromHelixNodeIdentifier(identifier), "my_host");
    assertEquals(Utils.parsePortFromHelixNodeIdentifier(identifier), port);

    identifier = "my_host_abc";
    assertEquals(Utils.parseHostFromHelixNodeIdentifier(identifier), "my_host");
    try {
      assertEquals(Utils.parsePortFromHelixNodeIdentifier(identifier), port);
      fail("Port should be numeric value");
    } catch (VeniceException e) {
      // expected
    }
  }

  @Test
  public void testGetDebugInfo() {
    Map<CharSequence, CharSequence> debugInfo = Utils.getDebugInfo();
    debugInfo.forEach((k, v) -> System.out.println(k + ": " + v));
    assertFalse(debugInfo.isEmpty(), "debugInfo should not be empty.");
    // N.B.: The "version" entry is not available in unit tests because of the way the classpath is built...
    String[] expectedKeys = { "path", "host", "pid", "user", "JDK major version" };
    assertTrue(
        debugInfo.size() >= expectedKeys.length,
        "debugInfo does not contain the minimum expected number of elements. Expected: " + expectedKeys.length
            + ". Actual: " + debugInfo.size() + ".");
    Arrays.stream(expectedKeys)
        .forEach(key -> assertTrue(debugInfo.containsKey(key), "debugInfo should contain: " + key));
  }

  @Test
  public void testMakeLargeNumberPretty() {
    Map<Long, String> inputOutput = new TreeMap<>();
    inputOutput.put(0L, "0");
    inputOutput.put(1L, "1");
    inputOutput.put(21L, "21");
    inputOutput.put(321L, "321");
    inputOutput.put(4321L, "4K");
    inputOutput.put(54321L, "54K");
    inputOutput.put(654321L, "654K");
    inputOutput.put(7654321L, "8M");
    inputOutput.put(87654321L, "88M");
    inputOutput.put(987654321L, "988M");
    inputOutput.put(1987654321L, "2B");
    inputOutput.put(21987654321L, "22B");
    inputOutput.put(321987654321L, "322B");
    inputOutput.put(4321987654321L, "4T");
    inputOutput.put(54321987654321L, "54T");
    inputOutput.put(654321987654321L, "654T");
    inputOutput.put(7654321987654321L, "7654T");
    inputOutput.put(87654321987654321L, "87654T");

    inputOutput.entrySet()
        .stream()
        .forEach(
            entry -> assertEquals(
                Utils.makeLargeNumberPretty(entry.getKey()),
                entry.getValue(),
                entry.getKey() + " does not get converted properly!"));
  }

  @Test
  public void testMakeTimePretty() {
    Map<Long, String> inputOutput = new TreeMap<>();
    inputOutput.put(0L, "0ns");
    inputOutput.put(1L, "1ns");
    inputOutput.put(21L, "21ns");
    inputOutput.put(321L, "321ns");
    inputOutput.put(4321L, "4us");
    inputOutput.put(54321L, "54us");
    inputOutput.put(654321L, "654us");
    inputOutput.put(7654321L, "8ms");
    inputOutput.put(87654321L, "88ms");
    inputOutput.put(987654321L, "988ms");
    inputOutput.put(1987654321L, "2.0s");
    inputOutput.put(21987654321L, "22.0s");
    inputOutput.put(321987654321L, "5.4m");
    inputOutput.put(4321987654321L, "1.2h");
    inputOutput.put(54321987654321L, "15.1h");
    inputOutput.put(654321987654321L, "181.8h");

    inputOutput.entrySet()
        .stream()
        .forEach(
            entry -> assertEquals(
                Utils.makeTimePretty(entry.getKey()),
                entry.getValue(),
                entry.getKey() + " does not get converted properly!"));
  }

  @Test
  public void testDirectoryExists() throws Exception {
    Path directoryPath = Files.createTempDirectory(null);
    Path filePath = Files.createTempFile(null, null);
    Path nonExistingPath = Paths.get(Utils.getUniqueTempPath());
    assertTrue(Utils.directoryExists(directoryPath.toString()));
    assertFalse(Utils.directoryExists(filePath.toString()));
    assertFalse(Utils.directoryExists(nonExistingPath.toString()));
    Files.delete(directoryPath);
    Files.delete(filePath);
  }

  @Test
  public void testIterateOnMapOfLists() {
    Map<String, List<Integer>> mapOfLists = new HashMap<>();
    mapOfLists.put("list1", new ArrayList<>());
    mapOfLists.put("list2", Arrays.asList(1, 2, 3));
    mapOfLists.put("list3", Arrays.asList(4, 5));
    mapOfLists.put("list4", Arrays.asList(6));
    List<Integer> expectedValues = Arrays.asList(1, 2, 3, 4, 5, 6);
    List<Integer> actualValues = new ArrayList<>();
    Iterator<Integer> iterator = Utils.iterateOnMapOfLists(mapOfLists);
    while (iterator.hasNext()) {
      actualValues.add(iterator.next());
    }
    actualValues.sort(Integer::compareTo);
    assertEquals(expectedValues, actualValues);
  }

  @Test
  public void testParseMap() {
    Map<String, String> expectedMap = new HashMap<>();
    expectedMap.put("a", "b");

    assertEquals(Utils.parseJsonMapFromString("", "test_field").size(), 0);
    Map<String, String> validMap = Utils.parseJsonMapFromString("{\"a\":\"b\"}", "test_field");
    assertEquals(validMap, expectedMap);

    VeniceException e = expectThrows(VeniceException.class, () -> Utils.parseJsonMapFromString("a=b", "test_field"));
    assertTrue(e.getMessage().contains("must be a valid JSON object"));

  }

  @Test
  public void testSanitizingStringForLogger() {
    assertEquals(Utils.getSanitizedStringForLogger(".abc.123."), "_abc_123_");
  }

  @Test
  public void testParseCommaSeparatedStringToSet() {
    Assert.assertTrue(Utils.parseCommaSeparatedStringToSet(null).isEmpty());
    Assert.assertTrue(Utils.parseCommaSeparatedStringToSet("").isEmpty());

    Set<String> set = Utils.parseCommaSeparatedStringToSet("a,b,c");
    assertEquals(set.size(), 3);
    Assert.assertTrue(set.contains("a"));
    Assert.assertTrue(set.contains("b"));
    Assert.assertTrue(set.contains("c"));

    Set<String> setWithSpaces = Utils.parseCommaSeparatedStringToSet("a, b, c");
    assertEquals(setWithSpaces.size(), 3);
    Assert.assertTrue(setWithSpaces.contains("a"));
    Assert.assertTrue(setWithSpaces.contains("b"));
    Assert.assertTrue(setWithSpaces.contains("c"));
  }

  @Test
  public void testParseCommaSeparatedStringToList() {
    Assert.assertTrue(Utils.parseCommaSeparatedStringToList(null).isEmpty());
    Assert.assertTrue(Utils.parseCommaSeparatedStringToList("").isEmpty());

    List<String> list = Utils.parseCommaSeparatedStringToList("a,b,c");
    assertEquals(list.size(), 3);
    assertEquals(list.get(0), "a");
    assertEquals(list.get(1), "b");
    assertEquals(list.get(2), "c");

    List<String> stringList = Utils.parseCommaSeparatedStringToList("a, b, c");
    assertEquals(stringList.size(), 3);
    assertEquals(list.get(0), "a");
    assertEquals(list.get(1), "b");
    assertEquals(list.get(2), "c");
  }

  @Test
  public void testResolveKafkaUrlForSepTopic() {
    String originalKafkaUrl = "localhost:12345";
    String originalKafkaUrlForSep = "localhost:12345_sep";
    assertEquals(Utils.resolveKafkaUrlForSepTopic(""), "");
    assertEquals(Utils.resolveKafkaUrlForSepTopic(originalKafkaUrlForSep), originalKafkaUrl);
    assertEquals(Utils.resolveKafkaUrlForSepTopic(originalKafkaUrl), originalKafkaUrl);
  }

  @Test
  void testGetRealTimeTopicNames() {
    String storeName = "StoreName";
    Store mockStore = mock(Store.class);
    List<Version> mockVersions = new ArrayList<>();
    mockVersions.add(mock(Version.class));
    mockVersions.add(mock(Version.class));
    mockVersions.add(mock(Version.class));
    HybridStoreConfig mockHybridConfig = mock(HybridStoreConfig.class);

    when(mockStore.getName()).thenReturn(storeName);
    when(mockStore.getVersions()).thenReturn(mockVersions);
    when(mockStore.getCurrentVersion()).thenReturn(1);
    when(mockStore.getHybridStoreConfig()).thenReturn(mockHybridConfig);
    when(mockVersions.get(0).getStoreName()).thenReturn(storeName);
    when(mockVersions.get(1).getStoreName()).thenReturn(storeName);
    when(mockVersions.get(2).getStoreName()).thenReturn(storeName);
    when(mockVersions.get(0).isHybrid()).thenReturn(true);
    when(mockVersions.get(1).isHybrid()).thenReturn(true);
    when(mockVersions.get(2).isHybrid()).thenReturn(true);
    when(mockVersions.get(0).getHybridStoreConfig()).thenReturn(mockHybridConfig);
    when(mockVersions.get(1).getHybridStoreConfig()).thenReturn(mockHybridConfig);
    when(mockVersions.get(2).getHybridStoreConfig()).thenReturn(mockHybridConfig);

    when(mockHybridConfig.getRealTimeTopicName()).thenReturn("StoreName_v1_rt", "StoreName_v2_rt", "StoreName_v3_rt");

    Set<String> result = Utils.getAllRealTimeTopicNames(mockStore);
    assertEquals(result, new HashSet<>(Arrays.asList("StoreName_v1_rt", "StoreName_v2_rt", "StoreName_v3_rt")));
  }

  @Test
  void testGetRealTimeTopicNameWithStore() {
    Store mockStore = mock(Store.class);
    List<Version> mockVersions = Collections.singletonList(mock(Version.class));
    HybridStoreConfig mockHybridConfig = mock(HybridStoreConfig.class);

    when(mockStore.getName()).thenReturn(STORE_NAME);
    when(mockStore.getVersions()).thenReturn(mockVersions);
    when(mockStore.getCurrentVersion()).thenReturn(1);
    when(mockStore.getHybridStoreConfig()).thenReturn(mockHybridConfig);

    when(mockHybridConfig.getRealTimeTopicName()).thenReturn("RealTimeTopic");

    String result = Utils.getRealTimeTopicName(mockStore);
    assertEquals(result, "RealTimeTopic");
  }

  @Test
  void testGetRealTimeTopicNameWithStoreInfo() {
    StoreInfo mockStoreInfo = mock(StoreInfo.class);
    List<Version> mockVersions = Collections.singletonList(mock(Version.class));
    HybridStoreConfig mockHybridConfig = mock(HybridStoreConfig.class);

    when(mockStoreInfo.getName()).thenReturn(STORE_NAME);
    when(mockStoreInfo.getVersions()).thenReturn(mockVersions);
    when(mockStoreInfo.getCurrentVersion()).thenReturn(1);
    when(mockStoreInfo.getHybridStoreConfig()).thenReturn(mockHybridConfig);

    when(mockHybridConfig.getRealTimeTopicName()).thenReturn("RealTimeTopic");

    String result = Utils.getRealTimeTopicName(mockStoreInfo);
    assertEquals(result, "RealTimeTopic");
  }

  @Test
  void testGetRealTimeTopicNameWithHybridConfig() {
    HybridStoreConfig mockHybridConfig = mock(HybridStoreConfig.class);

    when(mockHybridConfig.getRealTimeTopicName()).thenReturn("RealTimeTopic");
    String result = Utils.getRealTimeTopicName("TestStore", Collections.EMPTY_LIST, 1, mockHybridConfig);

    assertEquals(result, "RealTimeTopic");
  }

  @Test
  void testGetRealTimeTopicNameWithoutHybridConfig() {
    String result = Utils.getRealTimeTopicName(STORE_NAME, Collections.EMPTY_LIST, 0, null);
    assertEquals(result, STORE_NAME + Version.REAL_TIME_TOPIC_SUFFIX);
  }

  @Test
  void testGetRealTimeTopicNameWithConflictingVersions() {
    Version mockVersion1 = mock(Version.class);
    Version mockVersion2 = mock(Version.class);
    HybridStoreConfig mockConfig1 = mock(HybridStoreConfig.class);
    HybridStoreConfig mockConfig2 = mock(HybridStoreConfig.class);

    when(mockVersion1.isHybrid()).thenReturn(true);
    when(mockVersion2.isHybrid()).thenReturn(true);
    when(mockVersion1.getHybridStoreConfig()).thenReturn(mockConfig1);
    when(mockVersion2.getHybridStoreConfig()).thenReturn(mockConfig2);
    when(mockConfig1.getRealTimeTopicName()).thenReturn("RealTimeTopic1");
    when(mockConfig2.getRealTimeTopicName()).thenReturn("RealTimeTopic2");

    String result = Utils.getRealTimeTopicName(STORE_NAME, Lists.newArrayList(mockVersion1, mockVersion2), 1, null);
    assertTrue(result.equals("RealTimeTopic1") || result.equals("RealTimeTopic2"));
  }

  @Test
  void testGetRealTimeTopicNameWithHybridVersion() {
    Version mockVersion = mock(Version.class);
    HybridStoreConfig mockHybridConfig = mock(HybridStoreConfig.class);
    String expectedRealTimeTopicName = Utils.composeRealTimeTopic(STORE_NAME, 1);

    when(mockVersion.isHybrid()).thenReturn(true);
    when(mockVersion.getHybridStoreConfig()).thenReturn(mockHybridConfig);
    when(mockVersion.getStoreName()).thenReturn(STORE_NAME);
    when(mockHybridConfig.getRealTimeTopicName()).thenReturn(expectedRealTimeTopicName);

    String result = Utils.getRealTimeTopicName(mockVersion);
    assertEquals(result, expectedRealTimeTopicName);
  }

  @Test
  void testGetRealTimeTopicNameWithNonHybridVersion() {
    // Mocking the Version object
    Version mockVersion = mock(Version.class);

    // Mock setup to trigger the exception path
    when(mockVersion.isHybrid()).thenReturn(false);
    when(mockVersion.getStoreName()).thenReturn(STORE_NAME);
    String result = Utils.getRealTimeTopicName(mockVersion);
    assertEquals(result, STORE_NAME + Version.REAL_TIME_TOPIC_SUFFIX);
  }

  @Test
  public void testParseDateTimeToEpoch() throws Exception {
    // Case 1: Valid Input
    String dateTimePst = "2024-12-02 15:30:00";
    String dateTimeUtc = "2024-12-02 23:30:00";
    String format = "yyyy-MM-dd HH:mm:ss";
    String timeZone = "America/Los_Angeles";
    long expectedEpoch = 1733182200000L;

    long epochTime = Utils.parseDateTimeToEpoch(dateTimePst, format, timeZone);
    assertEquals(epochTime, expectedEpoch, "The epoch time does not match the expected value.");

    // Case 2: Invalid Date Format
    assertThrows(ParseException.class, () -> Utils.parseDateTimeToEpoch("2024-12-02T15:30:00", format, timeZone));

    // Case 3: Invalid Time Zone; fallback to GMT
    long gmtEpochTime = Utils.parseDateTimeToEpoch(dateTimeUtc, format, "InvalidTimeZone");
    assertEquals(gmtEpochTime, expectedEpoch, "The epoch time does not match the expected value for GMT.");

    // Case 4: Different Time Zone
    String utcTimeZone = "UTC";
    long utcEpochTime = Utils.parseDateTimeToEpoch(dateTimeUtc, format, utcTimeZone);
    assertEquals(utcEpochTime, expectedEpoch, "The epoch time does not match the expected value for UTC.");
  }

  @Test
  public void testIsSeparateTopicRegion() {
    Assert.assertTrue(Utils.isSeparateTopicRegion("dc-0_sep"));
    assertFalse(Utils.isSeparateTopicRegion("dc-0"));
  }

  @Test
  public void testGetLeaderTopicFromPubSubTopic() {
    PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
    String store = "test_store";
    PubSubTopic versionTopic = pubSubTopicRepository.getTopic(Version.composeKafkaTopic(store, 1));
    PubSubTopic realTimeTopic = pubSubTopicRepository.getTopic(Utils.composeRealTimeTopic(store));
    PubSubTopic separateRealTimeTopic =
        pubSubTopicRepository.getTopic(Utils.getSeparateRealTimeTopicName(realTimeTopic.getName()));
    Assert.assertEquals(Utils.resolveLeaderTopicFromPubSubTopic(pubSubTopicRepository, versionTopic), versionTopic);
    Assert.assertEquals(Utils.resolveLeaderTopicFromPubSubTopic(pubSubTopicRepository, realTimeTopic), realTimeTopic);
    Assert.assertEquals(
        Utils.resolveLeaderTopicFromPubSubTopic(pubSubTopicRepository, separateRealTimeTopic),
        realTimeTopic);
  }

  @DataProvider(name = "booleanParsingData")
  public Object[][] booleanParsingData() {
    return new Object[][] {
        // Valid cases
        { "true", "testField", true }, // Valid "true"
        { "false", "testField", false }, // Valid "false"
        { "TRUE", "testField", true }, // Valid case-insensitive "TRUE"
        { "FALSE", "testField", false }, // Valid case-insensitive "FALSE"

        // Invalid cases
        { "notABoolean", "testField", null }, // Invalid string
        { "123", "testField", null }, // Non-boolean numeric string
        { "", "testField", null }, // Empty string
        { null, "testField", null }, // Null input
    };
  }

  @DataProvider(name = "booleanOrFalseParsingData")
  public Object[][] booleanOrFalseParsingData() {
    return new Object[][] {
        // Valid cases
        { "true", "testField", true }, // Valid "true"
        { "false", "testField", false }, // Valid "false"
        { "TRUE", "testField", true }, // Valid case-insensitive "TRUE"
        { "FALSE", "testField", false }, // Valid case-insensitive "FALSE"
        { null, "testField", false }, // Null input

        // Invalid cases
        { "notABoolean", "testField", null }, // Invalid string
        { "123", "testField", null }, // Non-boolean numeric string
        { "", "testField", null }, // Empty string
    };
  }

  @Test(dataProvider = "booleanParsingData")
  public void testParseBooleanOrThrow(String value, String fieldName, Boolean expectedResult) {
    if (expectedResult != null) {
      // For valid cases
      boolean result = Utils.parseBooleanOrThrow(value, fieldName);
      assertEquals(result, (boolean) expectedResult, "Parsed boolean value does not match expected value.");
      return;
    }
    VeniceHttpException e = expectThrows(VeniceHttpException.class, () -> Utils.parseBooleanOrThrow(value, fieldName));
    assertEquals(e.getHttpStatusCode(), HttpStatus.SC_BAD_REQUEST, "Invalid status code.");
    if (value == null) {
      assertEquals(e.getMessage(), "Http Status 400 - testField must be a boolean, but value is null.");
    } else {
      assertEquals(
          e.getMessage(),
          "Http Status 400 - testField must be a boolean, but value: " + value + " is invalid.");
    }
    assertEquals(e.getErrorType(), ErrorType.BAD_REQUEST);
  }

  @Test(dataProvider = "booleanOrFalseParsingData")
  public void testParseBooleanOrFalse(String value, String fieldName, Boolean expectedResult) {
    // For valid cases
    if (expectedResult != null) {
      boolean result = Utils.parseBooleanOrFalse(value, fieldName);
      assertEquals(result, (boolean) expectedResult, "Parsed boolean value does not match expected value.");
      return;
    }
    // For invalid cases
    VeniceHttpException e = expectThrows(VeniceHttpException.class, () -> Utils.parseBooleanOrThrow(value, fieldName));
    assertEquals(e.getHttpStatusCode(), HttpStatus.SC_BAD_REQUEST, "Invalid status code.");
    assertEquals(e.getMessage(), "Http Status 400 - testField must be a boolean, but value: " + value + " is invalid.");
    assertEquals(e.getErrorType(), ErrorType.BAD_REQUEST);
  }

  @Test
  public void testGetAllSchemasFromResources() {

    // Protocols to test
    AvroProtocolDefinition[] avroProtocolDefinitions = new AvroProtocolDefinition[] {
        AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE, AvroProtocolDefinition.PARTITION_STATE,
        AvroProtocolDefinition.STORE_VERSION_STATE, AvroProtocolDefinition.SERVER_METADATA_RESPONSE,
        AvroProtocolDefinition.SERVER_STORE_PROPERTIES_PAYLOAD, AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE,
        AvroProtocolDefinition.PUSH_STATUS_SYSTEM_SCHEMA_STORE };

    for (AvroProtocolDefinition avroProtocolDefinition: avroProtocolDefinitions) {
      Map<Integer, Schema> schemaMap = Utils.getAllSchemasFromResources(avroProtocolDefinition);

      Assert.assertNotNull(schemaMap, avroProtocolDefinition.getClassName());
      Assert.assertNotEquals(schemaMap.size(), 0, avroProtocolDefinition.getClassName());
      if (avroProtocolDefinition.currentProtocolVersion.isPresent()) {
        Assert.assertEquals(
            schemaMap.get(avroProtocolDefinition.currentProtocolVersion.get()),
            avroProtocolDefinition.getCurrentProtocolVersionSchema(),
            avroProtocolDefinition.getClassName());
      }
    }
  }
}
