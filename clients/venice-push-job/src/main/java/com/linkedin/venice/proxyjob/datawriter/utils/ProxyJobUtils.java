package com.linkedin.venice.proxyjob.datawriter.utils;

import java.io.IOException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public final class ProxyJobUtils {
  private static final String FILE_NAME = "jobStatus";

  private ProxyJobUtils() {
  }

  public static void writeJobStatus(Path proxyJobStateDirPath, ProxyJobStatus status) throws IOException {
    FileSystem fs = proxyJobStateDirPath.getFileSystem(new Configuration());
    FSDataOutputStream out = fs.create(new Path(proxyJobStateDirPath, FILE_NAME));

    byte[] serializedStatus = SerializationUtils.serialize(status);

    out.write(serializedStatus);
    out.flush();
    out.close();
  }

  public static ProxyJobStatus getJobStatus(Path proxyJobStateDirPath) throws IOException {
    FileSystem fs = proxyJobStateDirPath.getFileSystem(new Configuration());
    Path filePath = new Path(proxyJobStateDirPath, FILE_NAME);
    if (!fs.exists(filePath)) {
      return null;
    }
    FSDataInputStream inputStream = fs.open(filePath);
    byte[] serializedStatus = IOUtils.toByteArray(inputStream);
    return SerializationUtils.deserialize(serializedStatus);
  }
}
