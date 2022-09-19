package com.linkedin.venice.hadoop;

import com.linkedin.venice.hadoop.utils.HadoopUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class was originally from Voldemort.
 * https://github.com/voldemort/voldemort/blob/master/contrib/hadoop-store-builder/src/java/voldemort/store/readonly/mr/serialization/JsonSequenceFileInputFormat.java
 */
public class VsonSequenceFileInputFormat extends SequenceFileInputFormat<BytesWritable, BytesWritable> {
  private static final Logger LOGGER = LogManager.getLogger(VsonSequenceFileInputFormat.class.getName());

  @Override
  protected FileStatus[] listStatus(JobConf job) throws IOException {
    String dirs = job.get("mapred.input.dir", "");
    String[] list = StringUtils.split(dirs);

    List<FileStatus> status = new ArrayList<>();
    for (String s: list) {
      status.addAll(getAllSubFileStatus(job, new Path(s)));
    }

    return status.toArray(new FileStatus[0]);
  }

  private List<FileStatus> getAllSubFileStatus(JobConf inputConf, Path filterMemberPath) throws IOException {
    List<FileStatus> list = new ArrayList<>();

    FileSystem fs = filterMemberPath.getFileSystem(inputConf);
    FileStatus[] subFiles = fs.listStatus(filterMemberPath);

    if (subFiles != null) {
      if (fs.isDirectory(filterMemberPath)) {
        for (FileStatus subFile: subFiles) {
          if (!HadoopUtils.shouldPathBeIgnored(subFile.getPath())) {
            list.addAll(getAllSubFileStatus(inputConf, subFile.getPath()));
          }
        }
      } else {
        if (subFiles.length > 0 && !HadoopUtils.shouldPathBeIgnored(subFiles[0].getPath())) {
          list.add(subFiles[0]);
        }
      }
    }

    return list;
  }
}
