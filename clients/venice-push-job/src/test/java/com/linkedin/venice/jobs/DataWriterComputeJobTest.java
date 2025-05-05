package com.linkedin.venice.jobs;

import static com.linkedin.venice.ConfigKeys.PASS_THROUGH_CONFIG_PREFIXES_LIST_KEY;
import static com.linkedin.venice.jobs.DataWriterComputeJob.PASS_THROUGH_CONFIG_PREFIXES;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;


public class DataWriterComputeJobTest {
  @Test
  public void testNoOverlappingPassThroughConfigPrefixes() {
    int passThroughPrefixListSize = PASS_THROUGH_CONFIG_PREFIXES.size();
    /**
     * The following logic will make sure there is no prefix that is a prefix of another prefix.
     */
    for (int i = 0; i < passThroughPrefixListSize; ++i) {
      for (int j = i + 1; j < passThroughPrefixListSize; ++j) {
        String prefixI = PASS_THROUGH_CONFIG_PREFIXES.get(i);
        String prefixJ = PASS_THROUGH_CONFIG_PREFIXES.get(j);
        if (prefixI.startsWith(prefixJ)) {
          throw new VeniceException("Prefix: " + prefixJ + " shouldn't be a prefix of another prefix: " + prefixI);
        }

        if (prefixJ.startsWith(prefixI)) {
          throw new VeniceException("Prefix: " + prefixI + " shouldn't be a prefix of another prefix: " + prefixJ);
        }
      }
    }
  }

  @Test
  public void testBasicPrefixPassThrough() {
    String prefix = "kafka.";
    String prefixedKey = prefix + "bootstrap.servers";
    String val1 = "localhost:9092";
    String nonPrefixedKey = "unrelated.config";
    Map<String, String> map = new HashMap<String, String>() {
      {
        put(prefixedKey, val1);
        put("unrelated.config", "ignoreMe");
      }
    };
    VeniceProperties props = new VeniceProperties(map);
    Map<String, String> result = new HashMap<>();

    DataWriterComputeJob.populateWithPassThroughConfigs(props, result::put, Collections.singletonList(prefix), null);
    assertEquals(result.get(prefixedKey), val1);
    assertFalse(result.containsKey(nonPrefixedKey));
  }

  @Test
  public void testOverridePrefixTakesEffect() {
    String prefix = "hadoop.";
    String keyWithoutThePrefix = "fs.defaultFS";
    String key = prefix + keyWithoutThePrefix;
    String val = "hdfs://cluster";
    Map<String, String> map = new HashMap<String, String>() {
      {
        put(key, val);
        put("some.other.key", "value");
      }
    };
    VeniceProperties props = new VeniceProperties(map);
    Map<String, String> result = new HashMap<>();

    DataWriterComputeJob.populateWithPassThroughConfigs(props, result::put, Collections.emptyList(), prefix);
    assertEquals(result.get(keyWithoutThePrefix), val);
    assertFalse(result.containsKey(key));
  }

  @Test
  public void testAdditionalPrefixTakesEffect() {
    String passThroughPrefix = "custom.prefix.";
    String key = passThroughPrefix + "foo";
    String val = "bar";
    Map<String, String> raw = new HashMap<>();
    raw.put(key, val);
    raw.put(PASS_THROUGH_CONFIG_PREFIXES_LIST_KEY, passThroughPrefix);
    VeniceProperties props = new VeniceProperties(raw);
    Map<String, String> result = new HashMap<>();

    DataWriterComputeJob
        .populateWithPassThroughConfigs(props, result::put, Collections.singletonList("random.prefix."), null);
    assertEquals(result.get(key), val);
  }

  @Test
  public void testDuplicateAvoidance() {
    String passThroughPrefix1 = "standard.prefix.";
    String passThroughPrefix2 = "standard.custom.";
    String key1 = passThroughPrefix1 + "foo";
    String key2 = passThroughPrefix2 + "foo";
    String val1 = "123";
    String val2 = "456";
    Map<String, String> raw = new HashMap<>();
    raw.put(key1, val1);
    raw.put(key2, val2);
    raw.put(PASS_THROUGH_CONFIG_PREFIXES_LIST_KEY, String.join(",", passThroughPrefix1, passThroughPrefix2));
    VeniceProperties props = new VeniceProperties(raw);
    List<String> prefixes = Arrays.asList(passThroughPrefix1, passThroughPrefix2);
    Map<String, String> result = new HashMap<>();

    DataWriterComputeJob.populateWithPassThroughConfigs(props, result::put, prefixes, null);
    assertEquals(result.get(key1), val1);
    assertEquals(result.get(key2), val2);
    assertFalse(result.containsKey(PASS_THROUGH_CONFIG_PREFIXES_LIST_KEY));
  }
}
