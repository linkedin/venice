package com.linkedin.venice.endToEnd;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.BackupStrategy;
import com.linkedin.venice.meta.IncrementalPushPolicy;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.system.store.MetaStoreDataType;
import com.linkedin.venice.systemstore.schemas.StoreMetaKey;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.TestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;
import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.LongBinaryOperator;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.linkedin.venice.hadoop.VenicePushJob.*;
import static com.linkedin.venice.system.store.MetaStoreWriter.*;
import static com.linkedin.venice.utils.TestPushUtils.*;

//TODO: write a H2VWrapper that can handle the whole flow

@Test(singleThreaded = true)
public abstract class TestBatch {
  private static final Logger LOGGER = LogManager.getLogger(TestBatch.class);
  protected static final int TEST_TIMEOUT = 60 * Time.MS_PER_SECOND;
  private static final int MAX_RETRY_ATTEMPTS = 3;
  private static final String STRING_SCHEMA = "\"string\"";
  protected static final String baseDataPath1 = Utils.getTempDataDirectory().getAbsolutePath();
  protected static final String baseDataPath2 = Utils.getTempDataDirectory().getAbsolutePath();

  protected VeniceClusterWrapper veniceCluster;

  public abstract VeniceClusterWrapper initializeVeniceCluster();

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    veniceCluster = initializeVeniceCluster();
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    if (veniceCluster != null) {
      veniceCluster.close();
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void storeWithNoVersionThrows400() {

    //Create store
    File inputDir = getTempDataDirectory();
    Schema keySchema = Schema.parse("\"string\"");
    Schema valueSchema = Schema.parse("\"string\"");
    Pair<Schema, Schema> schemas = new Pair<>(keySchema, valueSchema);
    String storeName = Utils.getUniqueString("store");
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Properties props = defaultH2VProps(veniceCluster, inputDirPath, storeName);
    createStoreForJob(veniceCluster, schemas.getFirst().toString(), schemas.getSecond().toString(), props).close();

    //Query store
    try(AvroGenericStoreClient avroClient = ClientFactory.getAndStartGenericAvroClient(ClientConfig.defaultGenericClientConfig(storeName)
        .setVeniceURL(veniceCluster.getRandomRouterURL()))) {
      try {
        Object value1 = avroClient.get("key1").get();
        Assert.fail("Single get request on store with no push should fail");
      } catch (Exception e){
        Assert.assertTrue(e.getMessage().contains("Please push data to that store"));
      }

      try {
        Set<String> keys = new HashSet<>();
        keys.add("key2");
        keys.add("key3");
        Object values = avroClient.batchGet(keys).get();
        Assert.fail("Batch get request on store with no push should fail");
      } catch (Exception e){
        Assert.assertTrue(e.getMessage().contains("Please push data to that store"));
      }

    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testDuplicateKey() throws Exception {
    try {
      testStoreWithDuplicateKeys(false);
      Assert.fail();
    } catch (VeniceException e) {
      //push is expected to fail because of duplicate key
    }

    testStoreWithDuplicateKeys(true);
  }

  private void testStoreWithDuplicateKeys(boolean isDuplicateKeyAllowed) throws Exception {
    testBatchStore(inputDir -> {
      Schema recordSchema = writeSimpleAvroFileWithDuplicateKey(inputDir);
      return new Pair<>(recordSchema.getField("id").schema(),
                        recordSchema.getField("name").schema());
        }, props -> {
          if (isDuplicateKeyAllowed) {
            props.setProperty(ALLOW_DUPLICATE_KEY, "true");
          }
        },
        (avroClient, vsonClient, metricsRepository) -> {
          if (!isDuplicateKeyAllowed) {
            Assert.fail("Push should have failed since duplicate keys are not allowed");
          } else {
            // If duplicate keys are allowed, then the reads for those keys should not be null
            for (int i = 0; i < 100; i ++) {
              if (i % 10 == 0) {
                Assert.assertNotNull(avroClient.get("0").get());
              } else {
                Assert.assertEquals(avroClient.get(Integer.toString(i)).get().toString(), "test_name" + i);
              }
            }
          }
        });
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testEmptyPush() throws Exception {
    testBatchStore(
        inputDir -> {
          Schema recordSchema = writeEmptyAvroFileWithUserSchema(inputDir);
          return new Pair<>(recordSchema.getField("id").schema(),
              recordSchema.getField("name").schema());
        },
        properties -> {
          properties.setProperty(VENICE_DISCOVER_URL_PROP, properties.getProperty(VENICE_URL_PROP));
          properties.setProperty(VENICE_URL_PROP, "invalid_venice_urls");
        },
        (avroClient, vsonClient, metricsRepository) -> {}
    );
  }

  @Test(timeOut = TEST_TIMEOUT, dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testDataPushWithSchemaWithAWrongDefault(boolean extendedSchemaValidationCheckEnabled) throws Exception {
    final int recordCnt = 100;
    Exception pushJobException = null;
    try {
      testBatchStore(inputDir -> {
        Schema recordSchema = writeSimpleAvroFileWithASchemaWithAWrongDefaultValue(inputDir, recordCnt);
        return new Pair<>(recordSchema.getField("key").schema(), recordSchema.getField("value").schema());
      }, properties -> {
        properties.setProperty(KEY_FIELD_PROP, "key");
        properties.setProperty(VALUE_FIELD_PROP, "value");
        properties.setProperty(EXTENDED_SCHEMA_VALIDITY_CHECK_ENABLED, Boolean.toString(extendedSchemaValidationCheckEnabled));
      }, (avroClient, vsonClient, metricsRepository) -> {
        for (int i = 0; i < recordCnt; i++) {
          Object valueObject = avroClient.get(Integer.toString(i)).get();
          Assert.assertTrue(valueObject instanceof GenericRecord,
              "The returned value must be a ''GenericRecord' for key: " + i);
          GenericRecord value = (GenericRecord) valueObject;
          Assert.assertEquals(value.get("id").toString(), Integer.toString(i));
          Assert.assertEquals(Float.valueOf(value.get("score").toString()), 100.0f);
        }
      });
    } catch (Exception e) {
      pushJobException = e;
    }
    if (extendedSchemaValidationCheckEnabled) {
      Assert.assertTrue(pushJobException != null && pushJobException.getMessage().contains("Invalid default"));
    } else {
      Assert.assertNull(pushJobException);
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testCompressingRecord() throws Exception {
    H2VValidator validator = (avroClient, vsonClient, metricsRepository) -> {
      //test single get
      for (int i = 1; i <= 100; i ++) {
        Assert.assertEquals(avroClient.get(Integer.toString(i)).get().toString(), "test_name_" + i);
      }

      //test batch get
      for (int i = 0; i < 10; i ++) {
        Set<String> keys = new HashSet<>();
        for (int j = 1; j <= 10; j ++) {
          keys.add(Integer.toString(i * 10 + j));
        }

        Map<CharSequence, CharSequence> values = (Map<CharSequence, CharSequence>) avroClient.batchGet(keys).get();
        Assert.assertEquals(values.size(), 10);

        for (int j = 1; j <= 10; j ++) {
          Assert.assertEquals(values.get(Integer.toString(i * 10 + j)).toString(), "test_name_" + ((i * 10) + j));
        }
      }
    };
    String storeName = testBatchStore(
        inputDir -> {
          Schema recordSchema = writeSimpleAvroFileWithUserSchema(inputDir, false);
          return new Pair<>(recordSchema.getField("id").schema(),
              recordSchema.getField("name").schema());
        },
        properties -> {
          /**
           * Here will use {@link VENICE_DISCOVER_URL_PROP} instead.
           */
          properties.setProperty(VENICE_DISCOVER_URL_PROP, properties.getProperty(VENICE_URL_PROP));
          properties.setProperty(VENICE_URL_PROP, "invalid_venice_urls");
        },
        validator, new UpdateStoreQueryParams().setCompressionStrategy(CompressionStrategy.GZIP));

    // Re-push with Kafka Input
    testRepush(storeName, validator);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testZstdCompressingAvroRecordCanFailWhenNoFallbackAvailable() throws Exception {
    testBatchStore(
        inputDir -> {
          Schema recordSchema = writeSimpleAvroFileWithUserSchema(inputDir, false);
          return new Pair<>(recordSchema.getField("id").schema(),
              recordSchema.getField("name").schema());
        },
        properties -> {
          /**
           * Here will use {@link VENICE_DISCOVER_URL_PROP} instead.
           */
          properties.setProperty(VENICE_DISCOVER_URL_PROP, properties.getProperty(VENICE_URL_PROP));
          properties.setProperty(VENICE_URL_PROP, "invalid_venice_urls");
        },
        (avroClient, vsonClient, metricsRepository) -> {
          //test single get. Can throw exception since no fallback available
          try {
            Assert.assertEquals(avroClient.get("1").get().toString(), "test_name_1");
          } catch (ExecutionException e) {
            String exceptionRegex = ".* Compressor not available for resource " + avroClient.getStoreName()
                + "\\. Dictionary not downloaded\\.\\n";
            boolean matchesExpectedMessage = e.getMessage().matches(exceptionRegex);
            if (!matchesExpectedMessage) {
              Assert.fail("Unexpected exception message", e);
            }
          }
        }, new UpdateStoreQueryParams().setCompressionStrategy(CompressionStrategy.ZSTD_WITH_DICT));
  }

  static H2VValidator getSimpleFileWithUserSchemaValidatorForZstd() {
    return (avroClient, vsonClient, metricsRepository) -> {
      // Wait for the first get to succeed. After the first one, the following gets must succeed without retry.
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, true,
          () -> Assert.assertEquals(avroClient.get(Integer.toString(1)).get().toString(), "test_name_1"));

      // test single get, starting from i = 2.
      for (int i = 2; i <= 100; i++) {
        Assert.assertEquals(avroClient.get(Integer.toString(i)).get().toString(), "test_name_" + i);
      }

      // test batch get.
      for (int i = 0; i < 10; i++) {
        Set<String> keys = new HashSet<>();
        for (int j = 1; j <= 10; j++) {
          keys.add(Integer.toString(i * 10 + j));
        }

        Map<CharSequence, CharSequence> values = (Map<CharSequence, CharSequence>) avroClient.batchGet(keys).get();
        Assert.assertEquals(values.size(), 10);

        for (int j = 1; j <= 10; j++) {
          Assert.assertEquals(values.get(Integer.toString(i * 10 + j)).toString(), "test_name_" + ((i * 10) + j));
        }
      }
    };
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testZstdCompressingAvroRecordWhenNoFallbackAvailableWithSleep() throws Exception {
    testBatchStore(
        inputDir -> {
          Schema recordSchema = writeSimpleAvroFileWithUserSchema(inputDir, false);
          return new Pair<>(recordSchema.getField("id").schema(),
              recordSchema.getField("name").schema());
        },
        properties -> {
          /**
           * Here will use {@link VENICE_DISCOVER_URL_PROP} instead.
           */
          properties.setProperty(VENICE_DISCOVER_URL_PROP, properties.getProperty(VENICE_URL_PROP));
          properties.setProperty(VENICE_URL_PROP, "invalid_venice_urls");
          properties.setProperty(ZSTD_COMPRESSION_LEVEL, String.valueOf(17));
        },
        getSimpleFileWithUserSchemaValidatorForZstd(),
        new UpdateStoreQueryParams().setCompressionStrategy(CompressionStrategy.ZSTD_WITH_DICT));
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testZstdCompressingAvroRecordWhenFallbackAvailable() throws Exception {
    // Running a batch push first.
    String storeName = testBatchStore(
        inputDir -> {
          Schema recordSchema = writeSimpleAvroFileWithUserSchema(inputDir, false);
          return new Pair<>(recordSchema.getField("id").schema(),
              recordSchema.getField("name").schema());
        },
        properties -> {
          /**
           * Here will use {@link VENICE_DISCOVER_URL_PROP} instead.
           */
          properties.setProperty(VENICE_DISCOVER_URL_PROP, properties.getProperty(VENICE_URL_PROP));
          properties.setProperty(VENICE_URL_PROP, "invalid_venice_urls");
        },
        getSimpleFileWithUserSchemaValidatorForZstd());

    // Then, enabling dictionary compression. After some time has passed, dictionary would have been downloaded and the new version should be served.
    testBatchStore(
        inputDir -> {
          Schema recordSchema = writeAlternateSimpleAvroFileWithUserSchema(inputDir, false);
          return new Pair<>(recordSchema.getField("id").schema(),
              recordSchema.getField("name").schema());
        },
        properties -> {
          /**
           * Here will use {@link VENICE_DISCOVER_URL_PROP} instead.
           */
          properties.setProperty(VENICE_DISCOVER_URL_PROP, properties.getProperty(VENICE_URL_PROP));
          properties.setProperty(VENICE_URL_PROP, "invalid_venice_urls");
        },
        (avroClient, vsonClient, metricsRepository) -> {
          // Sleeping to allow dictionary download before version switch.
          Utils.sleep(1000);
          //test single get
          for (int i = 1; i <= 100; i ++) {
            Assert.assertEquals(avroClient.get(Integer.toString(i)).get().toString(), "alternate_test_name_" + i);
          }

          //test batch get
          for (int i = 0; i < 10; i ++) {
            Set<String> keys = new HashSet<>();
            for (int j = 1; j <= 10; j++) {
              keys.add(Integer.toString(i * 10 + j));
            }

            Map<CharSequence, CharSequence> values = (Map<CharSequence, CharSequence>) avroClient.batchGet(keys).get();
            Assert.assertEquals(values.size(), 10);

            for (int j = 1; j <= 10; j++) {
              Assert.assertEquals(values.get(Integer.toString(i * 10 + j)).toString(), "alternate_test_name_" + ((i * 10) + j));
            }
          }
        }, storeName, new UpdateStoreQueryParams().setCompressionStrategy(CompressionStrategy.ZSTD_WITH_DICT), false);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testEarlyDeleteBackupStore() throws Exception {
    String storeName = testBatchStoreMultiVersionPush(inputDir -> {
      Schema recordSchema = writeSimpleAvroFileWithUserSchema(inputDir, false);
      return new Pair<>(recordSchema.getField("id").schema(),
          recordSchema.getField("name").schema());
    }, properties -> {}, (avroClient, vsonClient, metricsRepository) -> {
      //test single get
      for (int i = 1; i <= 100; i ++) {
        Assert.assertEquals(avroClient.get(Integer.toString(i)).get().toString(), "test_name_" + i);
      }
    }, new UpdateStoreQueryParams().setBackupStrategy(BackupStrategy.DELETE_ON_NEW_PUSH_START));


    // First version should be fully cleaned up, while newer versions should exists.
    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      String firstVersionDataPath1 = baseDataPath1 + "/rocksdb/" + storeName + "_v1";
      String secondVersionDataPath1 = baseDataPath1 + "/rocksdb/" + storeName + "_v2";
      String firstVersionDataPath2 = baseDataPath2 + "/rocksdb/" + storeName + "_v1";
      String secondVersionDataPath2 = baseDataPath2 + "/rocksdb/" + storeName + "_v2";
      File firstVersionDataFolder1 = new File(firstVersionDataPath1);
      File firstVersionDataFolder2 = new File(firstVersionDataPath2);
      Assert.assertFalse(firstVersionDataFolder1.exists() || firstVersionDataFolder2.exists());
      File secondVersionDataFolder1 = new File(secondVersionDataPath1);
      File secondVersionDataFolder2 = new File(secondVersionDataPath2);
      Assert.assertTrue(secondVersionDataFolder1.exists() || secondVersionDataFolder2.exists());
    });

  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testIncrementalPush() throws Exception {
    String storeName = testBatchStore(inputDir -> {
      Schema recordSchema = writeSimpleAvroFileWithUserSchema(inputDir);
      return new Pair<>(recordSchema.getField("id").schema(), recordSchema.getField("name").schema());
    }, properties -> {
    }, (avroClient, vsonClient, metricsRepository) -> {
      for (int i = 1; i <= 100; i++) {
        Assert.assertEquals(avroClient.get(Integer.toString(i)).get().toString(), "test_name_" + i);
      }
    }, new UpdateStoreQueryParams().setIncrementalPushEnabled(true));

    testBatchStore(inputDir -> {
      Schema recordSchema = writeSimpleAvroFileWithUserSchema2(inputDir);
      return new Pair<>(recordSchema.getField("id").schema(), recordSchema.getField("name").schema());
    }, properties -> {
      properties.setProperty(INCREMENTAL_PUSH, "true");
    }, (avroClient, vsonClient, metricsRepository) -> {
      for (int i = 51; i <= 150; i++) {
        Assert.assertEquals(avroClient.get(Integer.toString(i)).get().toString(), "test_name_" + (i * 2));
      }
    }, storeName, new UpdateStoreQueryParams().setIncrementalPushEnabled(true), false);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testIncrementalPushWithCompression() throws Exception {
    String storeName = testBatchStore(
        inputDir -> {
          Schema recordSchema = writeSimpleAvroFileWithUserSchema(inputDir, false);
          return new Pair<>(recordSchema.getField("id").schema(),
              recordSchema.getField("name").schema());
        },
        properties -> {
          /**
           * Here will use {@link VENICE_DISCOVER_URL_PROP} instead.
           */
          properties.setProperty(VENICE_DISCOVER_URL_PROP, properties.getProperty(VENICE_URL_PROP));
          properties.setProperty(VENICE_URL_PROP, "invalid_venice_urls");
        },
        getSimpleFileWithUserSchemaValidatorForZstd(),
        new UpdateStoreQueryParams()
            .setCompressionStrategy(CompressionStrategy.ZSTD_WITH_DICT)
            .setIncrementalPushEnabled(true)
            .setIncrementalPushPolicy(IncrementalPushPolicy.INCREMENTAL_PUSH_SAME_AS_REAL_TIME)
            .setHybridOffsetLagThreshold(10)
            .setHybridRewindSeconds(0));

    testBatchStore(inputDir -> {
      Schema recordSchema = writeSimpleAvroFileWithUserSchema2(inputDir);
      return new Pair<>(recordSchema.getField("id").schema(), recordSchema.getField("name").schema());
    }, properties -> {
      properties.setProperty(INCREMENTAL_PUSH, "true");
    }, (avroClient, vsonClient, metricsRepository) -> {
      for (int i = 51; i <= 150; i++) {
        Assert.assertEquals(avroClient.get(Integer.toString(i)).get().toString(), "test_name_" + (i * 2));
      }
    }, storeName, null, false);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testIncrementalPushWritesToRealTimeTopicWithPolicy() throws Exception {
    /**
     * N.B. This test has some flaky issues where it occasionally times out... It seems to be specific to
     *      the {@link TestBatchForIngestionIsolation} subclass, and manifests in the form of one of the
     *      three push jobs below timing out. Adding some logs just to try to weed out the relevant
     *      start/end boundaries in build logs since the CI seems to jumble them up sometimes, especially
     *      when flaky retries are involved...
     */
    double randomNumber = Math.random();
    String classAndFunctionName = getClass().getSimpleName() + ".testIncrementalPushWritesToRealTimeTopicWithPolicy()";
    String uniqueTestId = "attempt [" + randomNumber + "] of " + classAndFunctionName;
    LOGGER.info("Start of " + uniqueTestId);
    try {
      String storeName = testBatchStore(inputDir -> {
          Schema recordSchema = writeSimpleAvroFileWithUserSchema(inputDir);
          return new Pair<>(recordSchema.getField("id").schema(), recordSchema.getField("name").schema());
        }, properties -> {
        }, (avroClient, vsonClient, metricsRepository) -> {
          for (int i = 1; i <= 100; i++) {
            Assert.assertEquals(avroClient.get(Integer.toString(i)).get().toString(), "test_name_" + i);
          }
        }, new UpdateStoreQueryParams()
            .setAmplificationFactor(2)
            .setIncrementalPushEnabled(true)
            .setLeaderFollowerModel(true)
            .setChunkingEnabled(true)
            .setHybridOffsetLagThreshold(10)
            .setHybridRewindSeconds(0)
            .setIncrementalPushPolicy(IncrementalPushPolicy.INCREMENTAL_PUSH_SAME_AS_REAL_TIME)
      );

      testBatchStore(inputDir -> {
        Schema recordSchema = writeSimpleAvroFileWithUserSchema2(inputDir);
        return new Pair<>(recordSchema.getField("id").schema(), recordSchema.getField("name").schema());
      }, properties -> {
        properties.setProperty(INCREMENTAL_PUSH, "true");
      }, (avroClient, vsonClient, metricsRepository) -> {
        for (int i = 51; i <= 150; i++) {
          Assert.assertEquals(avroClient.get(Integer.toString(i)).get().toString(), "test_name_" + (i * 2));
        }
      }, storeName, null, false);

      testBatchStore(inputDir -> {
        Schema recordSchema = writeSimpleAvroFileWithUserSchema(inputDir);
        return new Pair<>(recordSchema.getField("id").schema(), recordSchema.getField("name").schema());
      }, properties -> {
      }, (avroClient, vsonClient, metricsRepository) -> {
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
          for (int i = 1; i <= 100; i++) {
            Assert.assertEquals(avroClient.get(Integer.toString(i)).get().toString(), "test_name_" + i);
          }
          for (int i = 101; i <= 150; i++) {
            Assert.assertNull(avroClient.get(Integer.toString(i)).get());
          }
        });
      }, storeName, null, false);
      LOGGER.info("Successful end of " + uniqueTestId);
    } catch (Throwable e) {
      LOGGER.error("Caught throwable in " + uniqueTestId, e);
      throw e;
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testLeaderFollowerStateModel() throws Exception {
    testBatchStore(inputDir -> {
      Schema recordSchema = writeSimpleAvroFileWithUserSchema(inputDir, false);
      return new Pair<>(recordSchema.getField("id").schema(),
          recordSchema.getField("name").schema());
    }, properties -> {}, (avroClient, vsonClient, metricsRepository) -> {
      //test single get
      for (int i = 1; i <= 100; i ++) {
        Assert.assertEquals(avroClient.get(Integer.toString(i)).get().toString(), "test_name_" + i);
      }
    }, new UpdateStoreQueryParams().setLeaderFollowerModel(true));
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testMetaStoreSchemaValidation() throws Exception {
    String storeName = testBatchStore(inputDir -> {
      Schema recordSchema = writeSimpleAvroFileWithUserSchema(inputDir, false);
      return new Pair<>(recordSchema.getField("id").schema(),
          recordSchema.getField("name").schema());
    },properties -> {}, (avroClient, vsonClient, metricsRepository) -> {
      //test single get
      for (int i = 1; i <= 100; i ++) {
        Assert.assertEquals(avroClient.get(Integer.toString(i)).get().toString(), "test_name_" + i);
      }
    }, new UpdateStoreQueryParams().setLeaderFollowerModel(true), true);

    String metaStoreName = VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName);
    //Query meta store
    try(AvroGenericStoreClient avroClient = ClientFactory.getAndStartGenericAvroClient(ClientConfig.defaultGenericClientConfig(metaStoreName)
        .setVeniceURL(veniceCluster.getRandomRouterURL()))) {
      try {
        StoreMetaKey key = MetaStoreDataType.VALUE_SCHEMAS_WRITTEN_PER_STORE_VERSION.getStoreMetaKey(new HashMap<String, String>() {{
          put(KEY_STRING_STORE_NAME, storeName);
          put(KEY_STRING_VERSION_NUMBER, Integer.toString(1));
        }});
        Object value = avroClient.get(key).get();
        System.out.println(value);
      } catch (Exception e) {
        Assert.fail("get request to fetch schema from meta store fails", e);
      }
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testKafkaInputBatchJob() throws Exception {
    H2VValidator validator = (avroClient, vsonClient, metricsRepository) -> {
      //test single get
      for (int i = 1; i <= 100; i ++) {
        Assert.assertEquals(avroClient.get(Integer.toString(i)).get().toString(), "test_name_" + i);
      }
    };
    String storeName = testBatchStore(inputDir -> {
      Schema recordSchema = writeSimpleAvroFileWithUserSchema(inputDir, false);
      return new Pair<>(recordSchema.getField("id").schema(),
          recordSchema.getField("name").schema());
    }, properties -> {}, validator);
    // Re-push with Kafka Input
    testRepush(storeName, validator);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testKafkaInputAAStore() throws Exception {
    H2VValidator validator = (avroClient, vsonClient, metricsRepository) -> {
      //test single get
      for (int i = 1; i <= 100; i ++) {
        Assert.assertEquals(avroClient.get(Integer.toString(i)).get().toString(), "test_name_" + i);
      }
    };
    ControllerClient controllerClient = new ControllerClient(veniceCluster.getClusterName(), veniceCluster.getRandomRouterURL());

    String storeName = testBatchStore(inputDir -> {
      Schema recordSchema = writeSimpleAvroFileWithUserSchema(inputDir, false);
      return new Pair<>(recordSchema.getField("id").schema(),
          recordSchema.getField("name").schema());
    }, properties -> {}, validator, new UpdateStoreQueryParams()
        .setLeaderFollowerModel(true)
        .setActiveActiveReplicationEnabled(true)
        .setHybridRewindSeconds(5)
        .setHybridOffsetLagThreshold(2)
        .setNativeReplicationEnabled(true));
    // Re-push with Kafka Input
    testRepush(storeName, validator);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testReducerCountValidation() throws Exception {
    H2VValidator validator = (avroClient, vsonClient, metricsRepository) -> {
      //test single get
      for (int i = 1; i <= 1; i ++) {
        Assert.assertEquals(avroClient.get(Integer.toString(i)).get().toString(), "test_name_" + i);
      }
    };
    String storeName = testBatchStore(inputDir -> {
      Schema recordSchema = writeSimpleAvroFileWithUserSchema(inputDir, false, 1);
      return new Pair<>(recordSchema.getField("id").schema(),
          recordSchema.getField("name").schema());
    }, properties -> {}, validator, new UpdateStoreQueryParams().setPartitionCount(3));

    // Re-push with Kafka Input
    testBatchStore(
        inputDir -> new Pair<>(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.NULL)),
        properties -> {
          properties.setProperty(SOURCE_KAFKA, "true");
          properties.setProperty(KAFKA_INPUT_TOPIC, Version.composeKafkaTopic(storeName, 1));
          properties.setProperty(KAFKA_INPUT_BROKER_URL, veniceCluster.getKafka().getAddress());
          /**
           * This is used to make sure the first mapper doesn't contain any real messages, but just control messages.
           * So that {@link com.linkedin.venice.hadoop.AbstractVeniceMapper#maybeSprayAllPartitions} won't be invoked.
           */
          properties.setProperty(KAFKA_INPUT_MAX_RECORDS_PER_MAPPER, "2");
        }, validator, storeName, new UpdateStoreQueryParams(), false);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testBatchFromETL() throws Exception {
    testBatchStore(inputDir -> {
      Schema recordSchema = writeETLFileWithUserSchema(inputDir, false);
      return new Pair<>(Schema.parse(ETL_KEY_SCHEMA_STRING), Schema.parse(ETL_VALUE_SCHEMA_STRING));
    }, properties -> {
      properties.setProperty(KEY_FIELD_PROP, "key");
      properties.setProperty(VALUE_FIELD_PROP, "value");
      properties.setProperty(SOURCE_ETL, "true");
    }, (avroClient, vsonClient, metricsRepository) -> {
      // test single get
      for (int i = 1; i <= 50; i ++) {
        GenericRecord key = new GenericData.Record(Schema.parse(ETL_KEY_SCHEMA_STRING));
        GenericRecord value = new GenericData.Record(Schema.parse(ETL_VALUE_SCHEMA_STRING));

        key.put("id", Integer.toString(i));
        value.put("name", "test_name_" + i);

        Assert.assertEquals(avroClient.get(key).get().toString(), value.toString());
      }

      for (int i = 51; i <= 100; i ++) {
        GenericRecord key = new GenericData.Record(Schema.parse(ETL_KEY_SCHEMA_STRING));

        key.put("id", Integer.toString(i));

        Assert.assertNull(avroClient.get(key).get());
      }
    });
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testBatchFromETLWithForUnionWithNullSchema() throws Exception {
    testBatchStore(inputDir -> {
      Schema recordSchema = writeETLFileWithUnionWithNullSchema(inputDir, false);
      return new Pair<>(Schema.parse(ETL_KEY_SCHEMA_STRING), Schema.parse(ETL_UNION_VALUE_SCHEMA_STRING_WITH_NULL));
    }, properties -> {
      properties.setProperty(KEY_FIELD_PROP, "key");
      properties.setProperty(VALUE_FIELD_PROP, "value");
      properties.setProperty(SOURCE_ETL, "true");
    }, (avroClient, vsonClient, metricsRepository) -> {
      // test single get
      for (int i = 1; i <= 25; i ++) {
        GenericRecord key = new GenericData.Record(Schema.parse(ETL_KEY_SCHEMA_STRING));

        key.put("id", Integer.toString(i));

        Assert.assertEquals(avroClient.get(key).get().toString(), "string_" + i);
      }

      for (int i = 26; i <= 50; i ++) {
        GenericRecord key = new GenericData.Record(Schema.parse(ETL_KEY_SCHEMA_STRING));

        key.put("id", Integer.toString(i));

        Assert.assertEquals(avroClient.get(key).get(), i);
      }

      for (int i = 51; i <= 100; i ++) {
        GenericRecord key = new GenericData.Record(Schema.parse(ETL_KEY_SCHEMA_STRING));

        key.put("id", Integer.toString(i));

        Assert.assertNull(avroClient.get(key).get());
      }
    });
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testBatchFromETLWithForUnionWithoutNullSchema() throws Exception {
    testBatchStore(inputDir -> {
      Schema recordSchema = writeETLFileWithUnionWithoutNullSchema(inputDir, false);
      return new Pair<>(Schema.parse(ETL_KEY_SCHEMA_STRING), Schema.parse(ETL_UNION_VALUE_SCHEMA_STRING_WITHOUT_NULL));
    }, properties -> {
      properties.setProperty(KEY_FIELD_PROP, "key");
      properties.setProperty(VALUE_FIELD_PROP, "value");
      properties.setProperty(SOURCE_ETL, "true");
    }, (avroClient, vsonClient, metricsRepository) -> {
      // test single get
      for (int i = 1; i <= 25; i ++) {
        GenericRecord key = new GenericData.Record(Schema.parse(ETL_KEY_SCHEMA_STRING));

        key.put("id", Integer.toString(i));

        Assert.assertEquals(avroClient.get(key).get().toString(), "string_" + i);
      }

      for (int i = 26; i <= 50; i ++) {
        GenericRecord key = new GenericData.Record(Schema.parse(ETL_KEY_SCHEMA_STRING));

        key.put("id", Integer.toString(i));

        Assert.assertEquals(avroClient.get(key).get(), i);
      }

      for (int i = 51; i <= 100; i ++) {
        GenericRecord key = new GenericData.Record(Schema.parse(ETL_KEY_SCHEMA_STRING));

        key.put("id", Integer.toString(i));

        Assert.assertNull(avroClient.get(key).get());
      }
    });
  }

  protected String testBatchStore(InputFileWriter inputFileWriter, Consumer<Properties> extraProps, H2VValidator dataValidator) throws Exception {
    return testBatchStore(inputFileWriter, extraProps, dataValidator, new UpdateStoreQueryParams());
  }

  private String testBatchStore(InputFileWriter inputFileWriter, Consumer<Properties> extraProps, H2VValidator dataValidator,
      UpdateStoreQueryParams storeParms) throws Exception {
    return testBatchStore(inputFileWriter, extraProps, dataValidator, null, storeParms, false, false);
  }

  private String testBatchStore(InputFileWriter inputFileWriter, Consumer<Properties> extraProps, H2VValidator dataValidator,
      UpdateStoreQueryParams storeParms, boolean createMetaSystemStore) throws Exception {
    return testBatchStore(inputFileWriter, extraProps, dataValidator, null, storeParms, false, false, createMetaSystemStore);
  }

  private String testBatchStoreMultiVersionPush(InputFileWriter inputFileWriter, Consumer<Properties> extraProps, H2VValidator dataValidator,
      UpdateStoreQueryParams storeParms) throws Exception {
    return testBatchStore(inputFileWriter, extraProps, dataValidator, null, storeParms, true, false);
  }

  private String testBatchStoreWithDerivedSchema(InputFileWriter inputFileWriter, Consumer<Properties> extraProps, H2VValidator dataValidator,
      UpdateStoreQueryParams storeParms) throws Exception {
    return testBatchStore(inputFileWriter, extraProps, dataValidator, null, storeParms, true, true);
  }

  private void testRepush(String storeName, H2VValidator dataValidator) throws Exception {
    for (String combiner: new String[]{"true", "false"}) {
      testBatchStore(
          inputDir -> new Pair<>(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.NULL)),
          properties -> {
            properties.setProperty(SOURCE_KAFKA, "true");
            properties.setProperty(KAFKA_INPUT_BROKER_URL, veniceCluster.getKafka().getAddress());
            properties.setProperty(KAFKA_INPUT_MAX_RECORDS_PER_MAPPER, "5");
            properties.setProperty(KAFKA_INPUT_COMBINER_ENABLED, combiner);
          },
          dataValidator, storeName, new UpdateStoreQueryParams(), false);
    }
  }

  private String testBatchStore(InputFileWriter inputFileWriter, Consumer<Properties> extraProps, H2VValidator dataValidator,
      String existingStore, UpdateStoreQueryParams storeParms, boolean multiPushJobs) throws Exception {
    return testBatchStore(inputFileWriter, extraProps, dataValidator, existingStore, storeParms, multiPushJobs, false);
  }

  private String testBatchStore(InputFileWriter inputFileWriter, Consumer<Properties> extraProps, H2VValidator dataValidator,
      String existingStore, UpdateStoreQueryParams storeParms, boolean multiPushJobs, boolean addDerivedSchema) throws Exception {
    return testBatchStore(inputFileWriter, extraProps, dataValidator, existingStore, storeParms, multiPushJobs, addDerivedSchema, false);
  }

  private String testBatchStore(InputFileWriter inputFileWriter, Consumer<Properties> extraProps, H2VValidator dataValidator,
      String existingStore, UpdateStoreQueryParams storeParams, boolean multiPushJobs, boolean addDerivedSchema,  boolean createMetaSystemStore) throws Exception {
    File inputDir = getTempDataDirectory();
    Pair<Schema, Schema> schemas = inputFileWriter.write(inputDir);
    String storeName = StringUtils.isEmpty(existingStore) ? Utils.getUniqueString("store") : existingStore;

    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Properties props = defaultH2VProps(veniceCluster, inputDirPath, storeName);
    extraProps.accept(props);

    if (StringUtils.isEmpty(existingStore)) {
      createStoreForJob(veniceCluster.getClusterName(), schemas.getFirst().toString(), schemas.getSecond().toString(), props,
          storeParams, addDerivedSchema).close();
    } else if (storeParams != null){
      updateStore(veniceCluster.getClusterName(), props, storeParams);
    }

    if (createMetaSystemStore) {
      try (ControllerClient controllerClient = new ControllerClient(veniceCluster.getClusterName(), veniceCluster.getRandomRouterURL())) {
        VersionCreationResponse versionCreationResponse = controllerClient.emptyPush(
            VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName), storeName, 10000);
        TestUtils.waitForNonDeterministicPushCompletion(versionCreationResponse.getKafkaTopic(), controllerClient, 10000,
            TimeUnit.SECONDS);
      }
    }

    TestPushUtils.runPushJob("Test Batch push job", props);

    if (multiPushJobs) {
      TestPushUtils.runPushJob("Test Batch push job 2", props);
      TestPushUtils.runPushJob("Test Batch push job 3", props);
    }

    veniceCluster.refreshAllRouterMetaData();

    MetricsRepository metricsRepository = new MetricsRepository();
    try (AvroGenericStoreClient avroClient = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName)
            .setVeniceURL(veniceCluster.getRandomRouterURL())
            .setMetricsRepository(metricsRepository)); // metrics only available for Avro client...
        AvroGenericStoreClient vsonClient = ClientFactory.getAndStartGenericAvroClient(
            ClientConfig.defaultVsonGenericClientConfig(storeName)
                .setVeniceURL(veniceCluster.getRandomRouterURL()))) {
      dataValidator.validate(avroClient, vsonClient, metricsRepository);
    }

    return storeName;
  }

  interface InputFileWriter {
    Pair<Schema, Schema> write(File inputDir) throws IOException;
  }

  interface H2VValidator {
    void validate(AvroGenericStoreClient avroClient, AvroGenericStoreClient vsonClient, MetricsRepository metricsRepository) throws Exception;
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testLargeValues() throws Exception {
    try {
      testStoreWithLargeValues(false);
      Assert.fail("Pushing large values with chunking disabled should fail.");
    } catch (VeniceException e) {
      //push is expected to fail because of large values
    }

    testStoreWithLargeValues(true);
  }

  @Test(timeOut = TEST_TIMEOUT * 3, dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testKafkaInputBatchJobWithLargeValues(boolean sendDirectControlMessage) throws Exception {
    String storeName = testStoreWithLargeValues(true);
    try {
      testKafkaInputBatchJobWithLargeValues(false, storeName, sendDirectControlMessage);
      Assert.fail("Re-pushing large values with chunking disabled should fail.");
    } catch (VeniceException e) {
      // Re-push is expected to fail
    }
    testKafkaInputBatchJobWithLargeValues(true, storeName, sendDirectControlMessage);
  }

  private void testKafkaInputBatchJobWithLargeValues(boolean enableChunkingOnPushJob, String storeName, Boolean sendDirectControlMessage) throws Exception {
    testStoreWithLargeValues(enableChunkingOnPushJob,
        properties -> {
          properties.setProperty(SOURCE_KAFKA, "true");
          properties.setProperty(VENICE_STORE_NAME_PROP, storeName);
          properties.setProperty(KAFKA_INPUT_BROKER_URL, veniceCluster.getKafka().getAddress());
          properties.setProperty(KAFKA_INPUT_MAX_RECORDS_PER_MAPPER, "5");
          properties.setProperty(SEND_CONTROL_MESSAGES_DIRECTLY, sendDirectControlMessage.toString());
        }, storeName);
  }

  private String testStoreWithLargeValues(boolean isChunkingAllowed) throws Exception {
    return testStoreWithLargeValues(isChunkingAllowed, properties -> {}, null);
  }

  private String testStoreWithLargeValues(boolean isChunkingAllowed, Consumer<Properties> extraProps, String existingStore) throws Exception {
    int maxValueSize = 3 * 1024 * 1024; // 3 MB apiece
    int numberOfRecords = 10;

    InputFileWriter inputFileWriter = inputDir -> {
      Schema recordSchema = writeSimpleAvroFileWithCustomSize(inputDir, numberOfRecords, 0, maxValueSize);
      return new Pair<>(recordSchema.getField("id").schema(),
          recordSchema.getField("name").schema());
    };

    H2VValidator dataValidator = (avroClient, vsonClient, metricsRepository) -> {
      Set<String> keys = new HashSet<>(10);

      // Single gets
      for (int i = 0; i < numberOfRecords; i++) {
        int expectedSize = maxValueSize / numberOfRecords * (i + 1);
        String key = Integer.toString(i);
        keys.add(key);
        char[] chars = new char[expectedSize];
        Arrays.fill(chars, Integer.toString(i).charAt(0));
        String expectedString = new String(chars);
        Utf8 expectedUtf8 = new Utf8(expectedString);

        LOGGER.info("About to query key: " + i);
        // This call often fails due to a race condition where the store is not perceived to exist yet
        Utf8 returnedUtf8Value = null;
        Integer attempts = 0;
        while(attempts < MAX_RETRY_ATTEMPTS)
        {
          try {
            returnedUtf8Value = (Utf8) avroClient.get(key).get();
            break;
          } catch (VeniceClientException e) {
            attempts++;
            if(attempts == MAX_RETRY_ATTEMPTS) {
              throw e;
            }
            // Give it a sec
            Thread.sleep(1000);
          }
        }

        Assert.assertNotNull(returnedUtf8Value, "Avro client returned null value for key: " + key + ".");
        LOGGER.info("Received value of size: " + returnedUtf8Value.length() + " for key: " + key);
        Assert.assertEquals(returnedUtf8Value.toString().substring(0, 1), key, "Avro value does not begin with the expected prefix.");
        Assert.assertEquals(returnedUtf8Value.length(), expectedSize, "Avro value does not have the expected size.");
        Assert.assertEquals(returnedUtf8Value, expectedUtf8, "The entire large value should be filled with the same char: " + key);

        String jsonValue = (String) vsonClient.get(key).get();
        Assert.assertNotNull(jsonValue, "VSON client returned null value for key: " + key + ".");
        Assert.assertEquals(jsonValue.substring(0, 1), key, "VSON value does not begin with the expected prefix.");
        Assert.assertEquals(jsonValue.length(), expectedSize, "VSON value does not have the expected size.");
        Assert.assertEquals(jsonValue, expectedString, "The entire large value should be filled with the same char: " + key);
      }

      // Batch-get
      Map<String, Utf8> utf8Results = (Map<String, Utf8>) avroClient.batchGet(keys).get();
      Map<String, String> jsonResults = (Map<String, String>) vsonClient.batchGet(keys).get();
      for (String key: keys) {
        int i = Integer.parseInt(key);
        int expectedSize = maxValueSize / numberOfRecords * (i + 1);
        char[] chars = new char[expectedSize];
        Arrays.fill(chars, key.charAt(0));
        String expectedString = new String(chars);
        Utf8 expectedUtf8 = new Utf8(expectedString);

        Utf8 returnedUtf8Value = utf8Results.get(key);
        Assert.assertNotNull(returnedUtf8Value, "Avro client returned null value for key: " + key + ".");
        LOGGER.info("Received value of size: " + returnedUtf8Value.length() + " for key: " + key);
        Assert.assertEquals(returnedUtf8Value.toString().substring(0, 1), key, "Avro value does not begin with the expected prefix.");
        Assert.assertEquals(returnedUtf8Value.length(), expectedSize, "Avro value does not have the expected size.");
        Assert.assertEquals(returnedUtf8Value, expectedUtf8, "The entire large value should be filled with the same char: " + key);

        String jsonValue = jsonResults.get(key);
        Assert.assertNotNull(jsonValue, "VSON client returned null value for key: " + key + ".");
        Assert.assertEquals(jsonValue.substring(0, 1), key, "VSON value does not begin with the expected prefix.");
        Assert.assertEquals(jsonValue.length(), expectedSize, "VSON value does not have the expected size.");
        Assert.assertEquals(jsonValue, expectedString, "The entire large value should be filled with the same char: " + key);
      }
    };
    if (existingStore == null) {
      return testBatchStore(inputFileWriter, extraProps, dataValidator, new UpdateStoreQueryParams().setChunkingEnabled(isChunkingAllowed));
    }
    return testBatchStore(inputFileWriter, extraProps, dataValidator, existingStore, new UpdateStoreQueryParams().setChunkingEnabled(isChunkingAllowed), false, false);
  }

  @Test(timeOut =  TEST_TIMEOUT)
  public void testRunJobWithSchemaThatContainsUnknownField() throws Exception {
    testBatchStore(inputDir -> {
          Schema recordSchema = writeSchemaWithUnknownFieldIntoAvroFile(inputDir);
          return new Pair<>(recordSchema.getField("key").schema(),
              recordSchema.getField("value").schema());
        }, props -> {
          props.setProperty(KEY_FIELD_PROP, "key");
          props.setProperty(VALUE_FIELD_PROP, "value");
        },
        (avroClient, vsonClient, metricsRepository) -> {
          String schemaWithoutSymbolDocStr = loadFileAsString("SchemaWithoutSymbolDoc.avsc");
          Schema schemaWithoutSymbolDoc = AvroCompatibilityHelper.parse(schemaWithoutSymbolDocStr);
          GenericRecord keyRecord = new GenericData.Record(schemaWithoutSymbolDoc.getField("key").schema());
          Schema sourceSchema = keyRecord.getSchema().getField("source").schema();
          keyRecord.put("memberId", (long)1);
          keyRecord.put("source", AvroCompatibilityHelper.newEnumSymbol(sourceSchema, testRecordType.OFFLINE.toString()));
          IndexedRecord value = (IndexedRecord) avroClient.get(keyRecord).get();
          Assert.assertEquals(value.get(0).toString(), "LOGO");
          Assert.assertEquals(value.get(1), 1);

          String schemaWithSymbolDocStr = loadFileAsString("SchemaWithSymbolDoc.avsc");
          Schema schemaWithSymbolDoc = AvroCompatibilityHelper.parse(schemaWithSymbolDocStr);
          GenericRecord keyRecord2 = new GenericData.Record(schemaWithSymbolDoc.getField("key").schema());
          keyRecord2.put("memberId", (long)2);
          keyRecord2.put("source", AvroCompatibilityHelper.newEnumSymbol(sourceSchema, testRecordType.NEARLINE.toString()));
          IndexedRecord value2 = (IndexedRecord) avroClient.get(keyRecord2).get();
          Assert.assertEquals(value2.get(0).toString(), "INDUSTRY");
          Assert.assertEquals(value2.get(1), 2);
        });
  }

  private static class StatCounter extends AtomicLong {
    final long initialValue;
    final LongBinaryOperator accumulator;

    public StatCounter(long initialValue, LongBinaryOperator accumulator) {
      super(initialValue);
      this.initialValue = initialValue;
      this.accumulator = accumulator;
    }
    public void add(long newDataPoint) {
      accumulateAndGet(newDataPoint, accumulator);
    }
    public void reset() {
      set(initialValue);
    }
  }
  private static class MaxLong extends StatCounter {
    public MaxLong() {
      super(Integer.MIN_VALUE, (left, right) -> Math.max(left, right));
    }
  }
  private static class MinLong extends StatCounter {
    public MinLong() {
      super(Integer.MAX_VALUE, (left, right) -> Math.min(left, right));
    }
  }
  private static class TotalLong extends StatCounter {
    public TotalLong() {
      super(0, (left, right) -> left + right);
    }
  }

  @Test(enabled = false) // disabled because performance testing is not very amenable to the unit test environment
  public void stressTestLargeMultiGet() throws Exception {
    int valueSize = 800;
    int numberOfRecords = 100000;
    testBatchStore(inputDir -> {
          Schema recordSchema = writeAvroFileWithManyFloatsAndCustomTotalSize(inputDir, numberOfRecords, valueSize, valueSize);
          return new Pair<>(recordSchema.getField("id").schema(),
              recordSchema.getField("name").schema());
        }, props -> {},
        (avroClient, vsonClient, metricsRepository) -> {
          // Batch-get

          String storeName = avroClient.getStoreName();

          int numberOfBatchesOfConcurrentCalls = 200;
          int numberOfConcurrentCallsPerBatch = 10;
          int numberOfCalls = numberOfConcurrentCallsPerBatch * numberOfBatchesOfConcurrentCalls;
          int keysPerCall = 1000;
          final StatCounter minQueryTimeMs = new MinLong();
          final StatCounter maxQueryTimeMs = new MaxLong();
          final StatCounter totalQueryTimeMs = new TotalLong();
          final StatCounter globalMinQueryTimeMs = new MinLong();
          final StatCounter globalMaxQueryTimeMs = new MaxLong();
          final StatCounter globalTotalQueryTimeMs = new TotalLong();
          CompletableFuture[] futures = new CompletableFuture[numberOfConcurrentCallsPerBatch];
          long firstQueryStartTime = System.currentTimeMillis();
          for (int call = 0; call < numberOfCalls; call++) {
            final int finalCall = call;
            Set<String> keys = new HashSet(keysPerCall);
            for (int key = 0; key < keysPerCall; key++) {
              int keyToQuery = (call * keysPerCall + key) % numberOfRecords;
              keys.add(String.valueOf(keyToQuery));
            }
            final long startTime = System.nanoTime();
            futures[call % numberOfConcurrentCallsPerBatch] = avroClient.batchGet(keys).thenAccept(o -> {
              long endTime = System.nanoTime();
              Map<String, Utf8> utf8Results = (Map<String, Utf8>) o;
              Assert.assertEquals(utf8Results.size(), keysPerCall, "Not enough records returned!");
              long queryTimeNs = endTime - startTime;
              long queryTimeMs = queryTimeNs / Time.NS_PER_MS;
              LOGGER.info("Call #" + finalCall + ": " + queryTimeNs + " ns (" + queryTimeMs + " ms).");
              minQueryTimeMs.add(queryTimeMs);
              maxQueryTimeMs.add(queryTimeMs);
              totalQueryTimeMs.add(queryTimeMs);
              globalMinQueryTimeMs.add(queryTimeMs);
              globalMaxQueryTimeMs.add(queryTimeMs);
              globalTotalQueryTimeMs.add(queryTimeMs);
            });
            if (call > 0 && call % numberOfConcurrentCallsPerBatch == 0) {
              CompletableFuture.allOf(futures).thenAccept(aVoid -> {
                LOGGER.info("Min query time: " + minQueryTimeMs + " ms.");
                LOGGER.info("Max query time: " + maxQueryTimeMs + " ms.");
                LOGGER.info("Average query time: " + (totalQueryTimeMs.get() / numberOfConcurrentCallsPerBatch) + " ms.");
                minQueryTimeMs.reset();
                maxQueryTimeMs.reset();
                totalQueryTimeMs.reset();
              }).get();
            }
          }
          CompletableFuture.allOf(futures).thenAccept(aVoid -> {
            long allQueriesFinishTime = System.currentTimeMillis();
            double totalQueryTime = allQueriesFinishTime - firstQueryStartTime;
            LOGGER.info("Total query time: " + totalQueryTime + " ms for "
                + numberOfCalls + " total queries in "
                + numberOfBatchesOfConcurrentCalls + " batches of "
                + numberOfConcurrentCallsPerBatch + " calls each.");
            DecimalFormat decimalFormat = new DecimalFormat("0.0");
            LOGGER.info("Throughput (per test metrics): " + decimalFormat.format(numberOfCalls / (totalQueryTime / 1000.0)) + " queries / sec");
            LOGGER.info("Global min query time (per test metrics): " + globalMinQueryTimeMs + " ms.");
            LOGGER.info("Global max query time (per test metrics): " + globalMaxQueryTimeMs + " ms.");
            LOGGER.info("Global average query time (per test metrics): " + (globalTotalQueryTimeMs.get() / numberOfCalls) + " ms.");

            Map<String, ? extends Metric> metrics = metricsRepository.metrics();
            String metricPrefix = "." + storeName + "--" + RequestType.MULTI_GET.getMetricPrefix();

            Metric requestSerializationTimeMetric = metrics.get(metricPrefix + "request_serialization_time.Avg");
            Metric requestSubmissionToResponseHandlingTimeMetric = metrics.get(metricPrefix + "request_submission_to_response_handling_time.Avg");
            Metric responseDeserializationTimeMetric = metrics.get(metricPrefix + "response_deserialization_time.Avg");
            Metric responseEnvelopeDeserializationTimeMetric = metrics.get(metricPrefix + "response_envelope_deserialization_time.Avg");
            Metric responseRecordsDeserializationTimeMetric = metrics.get(metricPrefix + "response_records_deserialization_time.Avg");
            Metric responseRecordsDeserializationSubmissionToStartTime = metrics.get(metricPrefix + "response_records_deserialization_submission_to_start_time.Avg");

            Metric requestSerializationTimeMetric50 = metrics.get(metricPrefix + "request_serialization_time.50thPercentile");
            Metric requestSubmissionToResponseHandlingTimeMetric50 = metrics.get(metricPrefix + "request_submission_to_response_handling_time.50thPercentile");
            Metric responseDeserializationTimeMetric50 = metrics.get(metricPrefix + "response_deserialization_time.50thPercentile");
            Metric responseEnvelopeDeserializationTimeMetric50 = metrics.get(metricPrefix + "response_envelope_deserialization_time.50thPercentile");
            Metric responseRecordsDeserializationTimeMetric50 = metrics.get(metricPrefix + "response_records_deserialization_time.50thPercentile");
            Metric responseRecordsDeserializationSubmissionToStartTime50 = metrics.get(metricPrefix + "response_records_deserialization_submission_to_start_time.50thPercentile");

            Metric requestSerializationTimeMetric99 = metrics.get(metricPrefix + "request_serialization_time.99thPercentile");
            Metric requestSubmissionToResponseHandlingTimeMetric99 = metrics.get(metricPrefix + "request_submission_to_response_handling_time.99thPercentile");
            Metric responseDeserializationTimeMetric99 = metrics.get(metricPrefix + "response_deserialization_time.99thPercentile");
            Metric responseEnvelopeDeserializationTimeMetric99 = metrics.get(metricPrefix + "response_envelope_deserialization_time.99thPercentile");
            Metric responseRecordsDeserializationTimeMetric99 = metrics.get(metricPrefix + "response_records_deserialization_time.99thPercentile");
            Metric responseRecordsDeserializationSubmissionToStartTime99 = metrics.get(metricPrefix + "response_records_deserialization_submission_to_start_time.99thPercentile");

            Metric latencyMetric50 = metrics.get(metricPrefix + "healthy_request_latency.50thPercentile");
            Metric latencyMetric90 = metrics.get(metricPrefix + "healthy_request_latency.90thPercentile");
            Metric latencyMetric99 = metrics.get(metricPrefix + "healthy_request_latency.99thPercentile");


            LOGGER.info("Request serialization time                       (Avg, p50, p99) : "
                + Utils.round(requestSerializationTimeMetric.value(), 1) + " ms, \t"
                + Utils.round(requestSerializationTimeMetric50.value(), 1) + " ms, \t"
                + Utils.round(requestSerializationTimeMetric99.value(), 1) + " ms.");
            LOGGER.info("Request submission to response time              (Avg, p50, p99) : "
                + Utils.round(requestSubmissionToResponseHandlingTimeMetric.value(), 1) + " ms, \t"
                + Utils.round(requestSubmissionToResponseHandlingTimeMetric50.value(), 1) + " ms, \t"
                + Utils.round(requestSubmissionToResponseHandlingTimeMetric99.value(), 1) + " ms.");
            LOGGER.info("Response deserialization time                    (Avg, p50, p99) : "
                + Utils.round(responseDeserializationTimeMetric.value(), 1) + " ms, \t"
                + Utils.round(responseDeserializationTimeMetric50.value(), 1) + " ms, \t"
                + Utils.round(responseDeserializationTimeMetric99.value(), 1) + " ms.");
            LOGGER.info("Response envelope deserialization time           (Avg, p50, p99) : "
                + Utils.round(responseEnvelopeDeserializationTimeMetric.value(), 1) + " ms, \t"
                + Utils.round(responseEnvelopeDeserializationTimeMetric50.value(), 1) + " ms, \t"
                + Utils.round(responseEnvelopeDeserializationTimeMetric99.value(), 1) + " ms.");
            LOGGER.info("Response records deserialization time            (Avg, p50, p99) : "
                + Utils.round(responseRecordsDeserializationTimeMetric.value(), 9) + " ms, \t"
                + Utils.round(responseRecordsDeserializationTimeMetric50.value(), 9) + " ms, \t"
                + Utils.round(responseRecordsDeserializationTimeMetric99.value(), 9) + " ms.");
            LOGGER.info("Response records deserialization submission time (Avg, p50, p99) : "
                + Utils.round(responseRecordsDeserializationSubmissionToStartTime.value(), 9) + " ms, \t"
                + Utils.round(responseRecordsDeserializationSubmissionToStartTime50.value(), 9) + " ms, \t"
                + Utils.round(responseRecordsDeserializationSubmissionToStartTime99.value(), 9) + " ms.");
            LOGGER.info("Latency                                          (p50, p90, p99) : "
                + Utils.round(latencyMetric50.value(), 1) + " ms, \t"
                + Utils.round(latencyMetric90.value(), 1) + " ms, \t"
                + Utils.round(latencyMetric99.value(), 1) + " ms.");

          }).get();
        }, new UpdateStoreQueryParams().setBatchGetLimit(1000).setReadQuotaInCU(Integer.MAX_VALUE));
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testRunMRJobAndPBNJ() throws Exception {
    Utils.thisIsLocalhost();

    File inputDir = getTempDataDirectory();
    Schema recordSchema = writeSimpleAvroFileWithUserSchema(inputDir);
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("store");
    Properties props = defaultH2VProps(veniceCluster, inputDirPath, storeName);
    props.setProperty(VenicePushJob.PBNJ_ENABLE, "true");
    props.setProperty(VenicePushJob.PBNJ_ROUTER_URL_PROP, veniceCluster.getRandomRouterURL());
    createStoreForJob(veniceCluster.getClusterName(), recordSchema, props).close();

    try (VenicePushJob job = new VenicePushJob("Test push job", props)) {
      job.run();
      // Verify job properties
      Assert.assertEquals(job.getKafkaTopic(), Version.composeKafkaTopic(storeName, 1));
      Assert.assertEquals(job.getInputDirectory(), inputDirPath);
      String schema =
          "{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"example.avro\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"age\",\"type\":\"int\"}]}";
      Assert.assertEquals(job.getFileSchemaString(), schema);
      Assert.assertEquals(job.getKeySchemaString(), STRING_SCHEMA);
      Assert.assertEquals(job.getValueSchemaString(), STRING_SCHEMA);
      Assert.assertEquals(job.getInputFileDataSize(), 3872);

      veniceCluster.refreshAllRouterMetaData();

      // Verify the data in Venice Store
      String routerUrl = veniceCluster.getRandomRouterURL();
      try (AvroGenericStoreClient<String, Object> client = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerUrl))) {
        for (int i = 1; i <= 100; ++i) {
          String expected = "test_name_" + i;
          String actual = client.get(Integer.toString(i)).get().toString(); /* client.get().get() returns a Utf8 object */
          Assert.assertEquals(actual, expected);
        }

        try (ControllerClient controllerClient = new ControllerClient(veniceCluster.getClusterName(), routerUrl)) {
          JobStatusQueryResponse jobStatus = controllerClient.queryJobStatus(job.getKafkaTopic());
          Assert.assertFalse(jobStatus.isError(), "Error in getting JobStatusResponse: " + jobStatus.getError());
          Assert.assertEquals(jobStatus.getStatus(), ExecutionStatus.COMPLETED.toString(),
              "After job is complete, status should reflect that");
          // We won't verify progress any more here since we decided to disable this feature
        }
      }
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testKafkaInputBatchJobSucceedsWhenSourceTopicIsEmpty() throws Exception {
    H2VValidator emptyValidator = (avroClient, vsonClient, metricsRepository) -> {
    };

    //Run an Empty Push
    String storeName = testBatchStore(inputDir -> {
      Schema recordSchema = writeEmptyAvroFileWithUserSchema(inputDir);
      return new Pair<>(recordSchema.getField("id").schema(), recordSchema.getField("name").schema());
    }, properties -> {}, emptyValidator);

    //Verify the version is online
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      int version = veniceCluster.getRandmonVeniceController()
          .getVeniceAdmin()
          .getCurrentVersion(veniceCluster.getClusterName(), storeName);
      Assert.assertEquals(version, 1);
    });

    // Re-push with Kafka Input Format
    testBatchStore(inputDir -> new Pair<>(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.NULL)),
        properties -> {
          properties.setProperty(SOURCE_KAFKA, "true");
          properties.setProperty(KAFKA_INPUT_TOPIC, Version.composeKafkaTopic(storeName, 1));
          properties.setProperty(KAFKA_INPUT_BROKER_URL, veniceCluster.getKafka().getAddress());
          properties.setProperty(KAFKA_INPUT_MAX_RECORDS_PER_MAPPER, "5");
        }, emptyValidator, storeName, new UpdateStoreQueryParams(), false);

    //Verify the previous repush succeeded and new version is online
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      int version = veniceCluster.getRandmonVeniceController()
          .getVeniceAdmin()
          .getCurrentVersion(veniceCluster.getClusterName(), storeName);
      Assert.assertEquals(version, 2);
    });
  }
}
