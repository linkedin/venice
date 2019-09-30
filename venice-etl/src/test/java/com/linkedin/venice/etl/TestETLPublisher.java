package com.linkedin.venice.etl;

import com.linkedin.venice.etl.publisher.VeniceETLPublisher;
import com.linkedin.venice.exceptions.UndefinedPropertyException;
import com.linkedin.venice.utils.Time;
import java.util.Properties;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.linkedin.venice.etl.source.VeniceKafkaSource.*;


public class TestETLPublisher {
  private static final int TEST_TIMEOUT = 10 * Time.MS_PER_SECOND;

  @Test(timeOut = TEST_TIMEOUT,
      expectedExceptions = UndefinedPropertyException.class)
  public void testPublisherWithMissingProperties() throws Exception {
    Properties properties = new Properties();
    /**
     * Properties is missing ETL_SNAPSHOT_SOURCE_DIR and ETL_SNAPSHOT_DESTINATION_DIR.
     */
    properties.setProperty(VENICE_CONTROLLER_URLS, "http://localhost:1736");
    properties.setProperty(FABRIC_NAME, "ei-ltx1");
    VeniceETLPublisher publisher = new VeniceETLPublisher("testJobId", properties);
    publisher.run();
  }

  @Test
  public void testSnapshotFormatUtil() {
    String goodSnapshotFormat1 = "1570183273436-PT-558022380";
    String goodSnapshotFormat2 = "1570269674164-PT-0";
    Assert.assertTrue(VeniceETLPublisher.isValidSnapshotPath(goodSnapshotFormat1));
    Assert.assertTrue(VeniceETLPublisher.isValidSnapshotPath(goodSnapshotFormat2));

    String badSnapshotFormat1 = "1570183273436-558022380";
    String badSnapshotFormat2 = "PT-558022380";
    String badSnapshotFormat3 = "1570183273436-PT-a";
    Assert.assertFalse(VeniceETLPublisher.isValidSnapshotPath(badSnapshotFormat1));
    Assert.assertFalse(VeniceETLPublisher.isValidSnapshotPath(badSnapshotFormat2));
    Assert.assertFalse(VeniceETLPublisher.isValidSnapshotPath(badSnapshotFormat3));
  }
}
