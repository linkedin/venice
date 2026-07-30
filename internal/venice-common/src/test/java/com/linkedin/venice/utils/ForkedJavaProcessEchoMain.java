package com.linkedin.venice.utils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;


/**
 * Top-level helper main-class used by {@link TestForkedJavaProcessArgFile} to verify that
 * {@link ForkedJavaProcess#exec} correctly forks a process and propagates program args and JVM properties via the
 * {@code @argfile} mechanism, even when the classpath is artificially huge.
 *
 * Writes its received args/properties to the file path passed as the first argument, instead of stdout, since
 * {@link ForkedJavaProcess} already consumes the forked process' stdout internally for logging purposes.
 */
public class ForkedJavaProcessEchoMain {
  public static void main(String[] args) throws IOException {
    String outputFilePath = args[0];
    String remainingArgs = String.join(",", java.util.Arrays.copyOfRange(args, 1, args.length));
    String content = "ARGS=" + remainingArgs + System.lineSeparator() + "PROP="
        + System.getProperty("forkedJavaProcessArgFileTest.prop") + System.lineSeparator();
    Files.write(Paths.get(outputFilePath), content.getBytes(StandardCharsets.UTF_8));
  }
}
