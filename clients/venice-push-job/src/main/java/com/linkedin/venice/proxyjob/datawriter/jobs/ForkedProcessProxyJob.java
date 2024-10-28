package com.linkedin.venice.proxyjob.datawriter.jobs;

import com.linkedin.venice.utils.ForkedJavaProcess;
import java.util.Collections;
import java.util.List;


public class ForkedProcessProxyJob extends AbstractDataWriterProxyJob {
  @Override
  protected void runProxyJob(List<String> args) {
    try {
      ForkedJavaProcess javaProcess =
          ForkedJavaProcess.exec(DataWriterProxyDriver.class, args, Collections.emptyList());
      javaProcess.waitFor();
    } catch (Exception e) {
      throw new RuntimeException("Failed to run data writer job", e);
    }
  }
}
