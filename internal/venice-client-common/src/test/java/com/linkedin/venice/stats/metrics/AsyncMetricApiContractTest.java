package com.linkedin.venice.stats.metrics;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.FileObject;
import javax.tools.ForwardingJavaFileManager;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileManager;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;
import org.testng.annotations.Test;


/** Checks at compile time that async gauges cannot be created without a {@link MetricScope} and resolvers. */
public class AsyncMetricApiContractTest {
  private static final String PREFIX = "import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;\n"
      + "import com.linkedin.venice.stats.metrics.*;\n"
      + "import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;\n"
      + "import com.linkedin.venice.server.VersionRole;\n" + "import com.linkedin.venice.read.RequestType;\n"
      + "import io.opentelemetry.api.common.Attributes;\n" + "import java.util.Map;\n" + "class GaugeContract {\n"
      + "  void configure(VeniceOpenTelemetryMetricsRepository repository, MetricEntity metric,\n"
      + "      MetricScope scope, Attributes attributes, Map<VeniceMetricsDimensions, String> dimensions) {\n";
  /** Negative snippets must fail on API resolution or access, not on syntax. */
  private static final Set<String> API_ERROR_CODES = new HashSet<>(
      Arrays.asList(
          "compiler.err.cant.apply.symbol",
          "compiler.err.cant.apply.symbols",
          "compiler.err.cant.resolve.location.args",
          "compiler.err.report.access",
          "compiler.err.cant.resolve.location"));
  private static final Set<String> PARSE_ERROR_CODES = new HashSet<>(
      Arrays.asList(
          "compiler.err.expected",
          "compiler.err.illegal.start.of.expr",
          "compiler.err.premature.eof",
          "compiler.err.not.stmt"));

  @Test
  public void testGaugeApiContract() throws IOException {
    String positive = "AsyncMetricEntityStateBase.createWithState(metric, repository, dimensions, attributes, scope,\n"
        + "    () -> 1L, value -> value);\n"
        + "AsyncMetricEntityStateOneEnum.create(metric, repository, dimensions, VersionRole.class, scope,\n"
        + "    role -> role, (state, role) -> 1L);\n"
        + "AsyncMetricEntityStateTwoEnums.create(metric, repository, dimensions,\n"
        + "    VersionRole.class, RequestType.class,\n"
        + "    scope, (role, request) -> role, (state, role, request) -> 1L);\n"
        + "repository.registerObservableGauge(metric, scope,\n"
        + "    observation -> observation.observe(attributes, () -> 1L, value -> value));";
    StringBuilder diagnostics = new StringBuilder();
    Set<String> diagnosticCodes = new HashSet<>();
    assertTrue(compiles(positive, diagnostics, diagnosticCodes), "Supported API must compile:\n" + diagnostics);

    String[] invalid = { "AsyncMetricEntityStateBase.create(metric, repository, dimensions, attributes, () -> 1L);",
        "AsyncMetricEntityStateBase.createWithState(metric, repository, dimensions, attributes,\n"
            + "    () -> 1L, value -> value);",
        "repository.registerObservableLongGauge(metric, measurement -> measurement.record(1L, attributes));",
        "repository.registerObservableDoubleGauge(metric, measurement -> measurement.record(1.0, attributes));",
        "repository.registerObservableGauge(metric,\n"
            + "    observation -> observation.observe(attributes, () -> 1L, value -> value));",
        "AsyncMetricEntityStateOneEnum.create(metric, repository, dimensions, VersionRole.class, scope);",
        "AsyncMetricEntityStateOneEnum.create(metric, repository, dimensions, VersionRole.class,\n"
            + "    role -> role, (state, role) -> 1L);",
        "AsyncMetricEntityStateTwoEnums.create(metric, repository, dimensions, VersionRole.class, RequestType.class,\n"
            + "    (role, request) -> role, (state, role, request) -> 1L);",
        "AsyncMetricEntityStateTwoEnums.create(metric, repository, dimensions,\n"
            + "    VersionRole.class, RequestType.class, scope);",
        "new AsyncMetricEntityState(metric, repository, dimensions, null, null, java.util.Collections.emptyList(),\n"
            + "    () -> 1L, value -> value, attributes) {};" };
    for (String body: invalid) {
      diagnostics.setLength(0);
      diagnosticCodes.clear();
      assertFalse(compiles(body, diagnostics, diagnosticCodes), "Unsupported API compiled: " + body);
      assertFalse(Collections.disjoint(diagnosticCodes, API_ERROR_CODES), body + "\n" + diagnostics);
      assertTrue(Collections.disjoint(diagnosticCodes, PARSE_ERROR_CODES), body + "\n" + diagnostics);
    }
  }

  private static boolean compiles(String body, StringBuilder diagnostics, Set<String> diagnosticCodes)
      throws IOException {
    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    assertNotNull(compiler, "API contract tests require a JDK");
    DiagnosticCollector<JavaFileObject> errors = new DiagnosticCollector<>();
    String source = "package contract;\n" + PREFIX + body + "\n  }\n}\n";
    JavaFileObject input =
        new SimpleJavaFileObject(URI.create("string:///contract/GaugeContract.java"), JavaFileObject.Kind.SOURCE) {
          @Override
          public CharSequence getCharContent(boolean ignoreEncodingErrors) {
            return source;
          }
        };
    StandardJavaFileManager standard = compiler.getStandardFileManager(errors, null, null);
    try (JavaFileManager outputs = new ForwardingJavaFileManager<StandardJavaFileManager>(standard) {
      @Override
      public JavaFileObject getJavaFileForOutput(
          Location location,
          String name,
          JavaFileObject.Kind kind,
          FileObject sibling) {
        return new SimpleJavaFileObject(URI.create("memory:///" + name.replace('.', '/') + kind.extension), kind) {
          @Override
          public OutputStream openOutputStream() {
            return new ByteArrayOutputStream();
          }
        };
      }
    }) {
      boolean compiled =
          compiler
              .getTask(
                  null,
                  outputs,
                  errors,
                  Arrays.asList(
                      "-proc:none",
                      "-source",
                      "8",
                      "-target",
                      "8",
                      "-classpath",
                      System.getProperty("java.class.path")),
                  null,
                  Collections.singletonList(input))
              .call();
      for (Diagnostic<? extends JavaFileObject> diagnostic: errors.getDiagnostics()) {
        if (diagnostic.getKind() == Diagnostic.Kind.ERROR) {
          diagnosticCodes.add(diagnostic.getCode());
          diagnostics.append(diagnostic).append('\n');
        }
      }
      return compiled;
    }
  }
}
