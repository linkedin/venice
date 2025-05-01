package com.linkedin.venice.hadoop.utils;

import static com.linkedin.venice.vpj.VenicePushJobConstants.PATH_FILTER;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
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
    assertTrue(HadoopUtils.shouldPathBeIgnored(new Path(ignoredPath)));
    assertFalse(HadoopUtils.shouldPathBeIgnored(new Path(validPath)));
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
    assertFalse(fs.exists(p));
  }

  @Test
  public void testCreateDirectoryWithPermission() throws IOException {
    FsPermission PERMISSION_777 = FsPermission.createImmutable((short) 0777);
    FsPermission PERMISSION_700 = FsPermission.createImmutable((short) 0700);

    // create a directory with permissions if not exist
    Path filePath = new Path(Utils.getUniqueString("/tmp/venice-test"));
    assertFalse(filePath.getFileSystem(new Configuration()).exists(filePath));
    HadoopUtils.createDirectoryWithPermission(filePath, PERMISSION_700);
    assertTrue(filePath.getFileSystem(new Configuration()).exists(filePath));
    Assert.assertEquals(
        filePath.getFileSystem(new Configuration()).getFileStatus(filePath).getPermission(),
        PERMISSION_700);

    // Update permission if a directory already exists with a different permission
    assertTrue(filePath.getFileSystem(new Configuration()).exists(filePath));
    HadoopUtils.createDirectoryWithPermission(filePath, PERMISSION_777);
    assertTrue(filePath.getFileSystem(new Configuration()).exists(filePath));
    Assert.assertEquals(
        filePath.getFileSystem(new Configuration()).getFileStatus(filePath).getPermission(),
        PERMISSION_777);

    // Update permission if a directory already exists with a different permission
    assertTrue(filePath.getFileSystem(new Configuration()).exists(filePath));
    HadoopUtils.createDirectoryWithPermission(filePath, PERMISSION_777);
    assertTrue(filePath.getFileSystem(new Configuration()).exists(filePath));
    Assert.assertEquals(
        filePath.getFileSystem(new Configuration()).getFileStatus(filePath).getPermission(),
        PERMISSION_777);

    // Do nothing if a directory already exists with the same permission
    HadoopUtils.createDirectoryWithPermission(filePath, PERMISSION_777);
    assertTrue(filePath.getFileSystem(new Configuration()).exists(filePath));
    Assert.assertEquals(
        filePath.getFileSystem(new Configuration()).getFileStatus(filePath).getPermission(),
        PERMISSION_777);
  }

  @Test
  public void testIsSequenceFile() throws IOException {
    File simpleVsonInputDir = Utils.getTempDataDirectory();
    TestWriteUtils.writeSimpleVsonFile(simpleVsonInputDir);
    assertTrue(internalCheckSequenceFile(simpleVsonInputDir));

    File complexVsonInputDir = Utils.getTempDataDirectory();
    TestWriteUtils.writeComplexVsonFile(complexVsonInputDir);
    assertTrue(internalCheckSequenceFile(complexVsonInputDir));

    File avroInputDir = Utils.getTempDataDirectory();
    TestWriteUtils.writeSimpleAvroFileWithIntToIntSchema(avroInputDir, 10);
    assertFalse(internalCheckSequenceFile(avroInputDir));
  }

  private boolean internalCheckSequenceFile(File inputDir) throws IOException {
    Path inputDirPath = new Path(inputDir.toString());
    FileSystem fs = inputDirPath.getFileSystem(new Configuration());
    FileStatus[] fileStatuses = fs.listStatus(inputDirPath, PATH_FILTER);
    if (fileStatuses == null || fileStatuses.length == 0) {
      throw new VeniceException("No data found at source path: " + inputDirPath);
    }
    return HadoopUtils.isSequenceFile(fs, fileStatuses[0].getPath());
  }
}
