package com.linkedin.venice.hadoop.input.recordreader.vson;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.spark.input.hdfs.TestSparkInputFromHdfs;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mockito.Mockito;
import org.testng.annotations.Test;


public class VeniceVsonFileIteratorTest {
  @Test
  public void testConstructorInitializationAndExceptions() throws IOException {
    // Setup: Temporary directory and files
    File tempDir = Utils.getTempDataDirectory("test-dir");
    File tempSeqFile = File.createTempFile("test-file", ".seq", tempDir);
    Path tempSeqFilePath = new Path(tempSeqFile.getAbsolutePath());
    File tempNonSeqFile = File.createTempFile("test-file", ".txt", tempDir);
    Path tempNonSeqFilePath = new Path(tempNonSeqFile.getAbsolutePath());

    FileSystem fileSystem = FileSystem.getLocal(new Configuration());
    VeniceVsonRecordReader mockRecordReader = Mockito.mock(VeniceVsonRecordReader.class);

    // Write valid data to the sequence file
    TestSparkInputFromHdfs.writeVsonFile(tempSeqFilePath, 1, 2);

    // Case 1: Valid FileSystem and Path
    VeniceVsonFileIterator iterator = new VeniceVsonFileIterator(fileSystem, tempSeqFilePath, mockRecordReader);
    assertNotNull(iterator);

    // Case 2: Null FileSystem
    Exception exception =
        expectThrows(VeniceException.class, () -> new VeniceVsonFileIterator(null, tempSeqFilePath, mockRecordReader));
    assertTrue(exception.getMessage().contains("FileSystem cannot be null"));

    // Case 3: Null Path
    Exception exception2 =
        expectThrows(VeniceException.class, () -> new VeniceVsonFileIterator(fileSystem, null, mockRecordReader));
    assertTrue(exception2.getMessage().contains("Path cannot be null"));

    // Case 4: Null RecordReader
    Exception exception3 =
        expectThrows(VeniceException.class, () -> new VeniceVsonFileIterator(fileSystem, tempSeqFilePath, null));
    assertTrue(exception3.getMessage().contains("RecordReader cannot be null"));

    // Case 4: Invalid SequenceFile
    expectThrows(
        VeniceException.class,
        () -> new VeniceVsonFileIterator(fileSystem, tempNonSeqFilePath, mockRecordReader));
  }

  @Test
  public void testNextAndGetCurrentKeyValue() throws IOException {
    // Setup: Temporary sequence file
    File tempDir = Utils.getTempDataDirectory("test-dir");
    File tempSeqFile = File.createTempFile("test-file", ".seq", tempDir);
    Path tempSeqFilePath = new Path(tempSeqFile.getAbsolutePath());
    FileSystem fileSystem = FileSystem.getLocal(new Configuration());
    VeniceVsonRecordReader mockRecordReader = Mockito.mock(VeniceVsonRecordReader.class);

    int totalRecords = 5;
    // Write valid data to the sequence file
    TestSparkInputFromHdfs.writeVsonFile(tempSeqFilePath, 1, totalRecords);

    VeniceVsonFileIterator vsonFileIterator = new VeniceVsonFileIterator(fileSystem, tempSeqFilePath, mockRecordReader);

    // Mock behavior for record reader
    when(mockRecordReader.getKeyBytes(any(), any())).thenReturn("mockKey".getBytes());
    when(mockRecordReader.getValueBytes(any(), any())).thenReturn("mockValue".getBytes());

    byte[] lastKey = null;
    byte[] lastValue = null;

    int expectedRecords = totalRecords;
    while (vsonFileIterator.next()) {
      lastKey = vsonFileIterator.getCurrentKey();
      lastValue = vsonFileIterator.getCurrentValue();
      assertNotNull(lastKey);
      assertNotNull(lastValue);
      expectedRecords -= 1;
    }
    assertEquals(expectedRecords, 0);

    // Case 2: No more records (simulate by mocking behavior)
    assertFalse(vsonFileIterator.next());
    assertNotNull(lastKey);
    assertNotNull(lastValue);
    assertEquals(lastKey, vsonFileIterator.getCurrentKey());
    assertEquals(lastValue, vsonFileIterator.getCurrentValue());
  }
}
