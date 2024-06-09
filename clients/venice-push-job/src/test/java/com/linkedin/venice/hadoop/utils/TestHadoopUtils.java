package com.linkedin.venice.hadoop.utils;

import com.linkedin.venice.utils.Utils;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
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
    // create the path
    Path p = new Path(path);
    FileSystem fs = p.getFileSystem(conf);
    if (!fs.exists(p)) {
      fs.mkdirs(p);
    }

    // clean up the path
    HadoopUtils.cleanUpHDFSPath(path, true);

    // validate the path
    Assert.assertFalse(fs.exists(p));
  }

  @Test
  public void testCreateDirectoryWithPermission() throws IOException {
    FsPermission PERMISSION_777 = FsPermission.createImmutable((short) 0777);
    FsPermission PERMISSION_700 = FsPermission.createImmutable((short) 0700);

    // create a directory with permissions if not exist
    Path filePath = new Path(Utils.getUniqueString("/tmp/venice-test"));
    Assert.assertFalse(filePath.getFileSystem(new Configuration()).exists(filePath));
    HadoopUtils.createDirectoryWithPermission(filePath, PERMISSION_700);
    Assert.assertTrue(filePath.getFileSystem(new Configuration()).exists(filePath));
    Assert.assertEquals(
        filePath.getFileSystem(new Configuration()).getFileStatus(filePath).getPermission(),
        PERMISSION_700);

    // Update permission if a directory already exists with a different permission
    Assert.assertTrue(filePath.getFileSystem(new Configuration()).exists(filePath));
    HadoopUtils.createDirectoryWithPermission(filePath, PERMISSION_777);
    Assert.assertTrue(filePath.getFileSystem(new Configuration()).exists(filePath));
    Assert.assertEquals(
        filePath.getFileSystem(new Configuration()).getFileStatus(filePath).getPermission(),
        PERMISSION_777);

    // Update permission if a directory already exists with a different permission
    Assert.assertTrue(filePath.getFileSystem(new Configuration()).exists(filePath));
    HadoopUtils.createDirectoryWithPermission(filePath, PERMISSION_777);
    Assert.assertTrue(filePath.getFileSystem(new Configuration()).exists(filePath));
    Assert.assertEquals(
        filePath.getFileSystem(new Configuration()).getFileStatus(filePath).getPermission(),
        PERMISSION_777);

    // Do nothing if a directory already exists with the same permission
    HadoopUtils.createDirectoryWithPermission(filePath, PERMISSION_777);
    Assert.assertTrue(filePath.getFileSystem(new Configuration()).exists(filePath));
    Assert.assertEquals(
        filePath.getFileSystem(new Configuration()).getFileStatus(filePath).getPermission(),
        PERMISSION_777);
  }
}
