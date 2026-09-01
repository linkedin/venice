package com.linkedin.venice.utils;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.Test;


/**
 * Tests for the {@code @argfile} mechanism in {@link ForkedJavaProcess#exec}, which avoids OS-level
 * "Argument list too long" (execve() E2BIG) failures that can otherwise occur when the classpath passed to a forked
 * JVM is very large.
 */
public class TestForkedJavaProcessArgFile {
  private static final String PROP_KEY = "forkedJavaProcessArgFileTest.prop";

  /**
   * Builds a classpath long enough to exceed a typical OS argv size limit if passed directly (instead of via
   * {@code @argfile}), by padding a valid classpath with a large number of fake entries.
   */
  private static String buildHugeClasspath() {
    StringBuilder hugeClasspath = new StringBuilder(ForkedJavaProcess.getClasspath());
    for (int i = 0; i < 20000; i++) {
      hugeClasspath.append(File.pathSeparatorChar)
          .append("/tmp/fake/very/long/gradle/cache/path/segment/")
          .append(i)
          .append("/some-fake-artifact-name-")
          .append(i)
          .append(".jar");
    }
    return hugeClasspath.toString();
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testForkWithHugeClasspathPropagatesArgsAndJvmProperties() throws Exception {
    if (!isArgFileSupported()) {
      // On JDK 8, the java launcher doesn't understand @argfile syntax at all, so ForkedJavaProcess falls back to
      // passing arguments directly; a huge classpath is then expected to still hit the OS argument-length limit,
      // same as it always has on JDK 8.
      throw new SkipException("The @argfile mechanism is only supported on JDK 9+.");
    }
    String hugeClasspath = buildHugeClasspath();
    File outputFile = File.createTempFile("forked-java-process-argfile-test-output", ".txt");
    outputFile.deleteOnExit();

    // Includes both a token requiring @argfile quoting ("hello world") and one that doesn't ("arg2"), as well as a
    // JVM property whose value contains whitespace, exercising both branches of the argfile token quoting logic.
    ForkedJavaProcess forked = ForkedJavaProcess.exec(
        ForkedJavaProcessEchoMain.class,
        Arrays.asList(outputFile.getAbsolutePath(), "hello world", "arg2"),
        Collections.singletonList("-D" + PROP_KEY + "=value with spaces"),
        hugeClasspath,
        true,
        Optional.empty());

    int exitCode = forked.waitFor();
    String output = new String(Files.readAllBytes(outputFile.toPath()), StandardCharsets.UTF_8);

    Assert.assertEquals(exitCode, 0, "Forked process should exit cleanly. Output was:\n" + output);
    Assert.assertTrue(output.contains("ARGS=hello world,arg2"), "Unexpected output: " + output);
    Assert.assertTrue(output.contains("PROP=value with spaces"), "Unexpected output: " + output);
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testArgFileIsCleanedUpAfterForkedProcessExits() throws Exception {
    File tempDir = Utils.getTempDataDirectory();
    long argFilesBefore = countArgFiles(tempDir);
    File outputFile = File.createTempFile("forked-java-process-argfile-test-output", ".txt");
    outputFile.deleteOnExit();

    ForkedJavaProcess forked = ForkedJavaProcess.exec(
        ForkedJavaProcessEchoMain.class,
        Collections.singletonList(outputFile.getAbsolutePath()),
        Collections.emptyList(),
        false);
    Assert.assertEquals(forked.waitFor(), 0);

    TestUtils.waitForNonDeterministicAssertion(
        10,
        TimeUnit.SECONDS,
        () -> Assert.assertEquals(
            countArgFiles(tempDir),
            argFilesBefore,
            "The @argfile created for the forked process should be deleted once the process exits."));
  }

  private static long countArgFiles(File tempDir) {
    File[] files = tempDir.listFiles((dir, name) -> name.startsWith("forked-java-process-args"));
    return files == null ? 0 : files.length;
  }

  /**
   * Mirrors {@code ForkedJavaProcess.isArgFileSupported()}: the java launcher only understands {@code @argfile}
   * syntax starting with JDK 9.
   */
  private static boolean isArgFileSupported() {
    String specVersion = System.getProperty("java.specification.version", "");
    if (specVersion.startsWith("1.")) {
      return false;
    }
    try {
      return Integer.parseInt(specVersion) >= 9;
    } catch (NumberFormatException e) {
      return true;
    }
  }
}
