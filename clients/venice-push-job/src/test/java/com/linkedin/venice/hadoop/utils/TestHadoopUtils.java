package com.linkedin.venice.hadoop.utils;

import com.linkedin.venice.utils.VeniceProperties;
import java.io.IOException;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestHadoopUtils {
  @Test
  public void testGetProps() {
    JobConf conf = new JobConf();
    Assert.assertNotNull(HadoopUtils.getProps(conf));
  }

  @Test
  public void testShouldPathBeIgnored() throws IOException {
    String validPath = "/test", ignoredPath = "/_test";
    Assert.assertTrue(HadoopUtils.shouldPathBeIgnored(new Path(ignoredPath)));
    Assert.assertFalse(HadoopUtils.shouldPathBeIgnored(new Path(validPath)));
  }

  @Test
  public void testCleanUpHDFSPath() throws IOException {
    String path = "/tmp/venice-test/";
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    // create the path
    Path p = new Path(path);
    if (!fs.exists(p)) {
      fs.mkdirs(p);
    }

    // clean up the path
    HadoopUtils.cleanUpHDFSPath(path, true);

    // validate the path
    Assert.assertFalse(fs.exists(p));
  }

  @Test
  public void testSetHadoopConfigurationFromProperties() {
    Configuration conf = new Configuration();
    Properties innerProps = new Properties();
    innerProps.setProperty("non.valid.key", "shouldn't exist");
    innerProps.setProperty("hadoop-conf.fs.s3a.access.key", "s3-key");
    VeniceProperties props = new VeniceProperties(innerProps);
    HadoopUtils.setHadoopConfigurationFromProperties(conf, props);
    Assert.assertEquals(conf.get("fs.s3a.access.key"), "s3-key");
    Assert.assertNull(conf.get("non.valid.key"));
  }
}
