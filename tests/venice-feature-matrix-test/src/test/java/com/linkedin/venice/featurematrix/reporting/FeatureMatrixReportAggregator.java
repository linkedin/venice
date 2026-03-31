package com.linkedin.venice.featurematrix.reporting;

import com.linkedin.venice.featurematrix.model.FeatureDimensions.DimensionId;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Aggregates test failures and generates reports grouped by:
 * 1. Dimension - which dimension values are associated with failures
 * 2. Component - which components (Router, Server, Controller, etc.) are failing
 * 3. Dimension pairs - which dimension-value combinations have interaction bugs
 * 4. Write vs Read - separating ingestion issues from serving issues
 */
public class FeatureMatrixReportAggregator {
  private static final Logger LOGGER = LogManager.getLogger(FeatureMatrixReportAggregator.class);

  private final List<FailureReport> failures = new CopyOnWriteArrayList<>();
  private int totalTestCount = 0;
  private int passedCount = 0;
  private int failedCount = 0;
  private int skippedCount = 0;

  public void addFailure(FailureReport failure) {
    failures.add(failure);
  }

  public void recordCounts(int total, int passed, int failed, int skipped) {
    this.totalTestCount = total;
    this.passedCount = passed;
    this.failedCount = failed;
    this.skippedCount = skipped;
  }

  /**
   * Groups failures by each dimension value.
   * Returns map of "DimensionId=value" -> list of failures.
   */
  public Map<String, List<FailureReport>> groupByDimension() {
    Map<String, List<FailureReport>> groups = new LinkedHashMap<>();
    for (FailureReport f: failures) {
      for (Map.Entry<DimensionId, String> entry: f.getDimensions().entrySet()) {
        String key = entry.getKey().name() + "=" + entry.getValue();
        groups.computeIfAbsent(key, k -> new ArrayList<>()).add(f);
      }
    }
    return groups;
  }

  /**
   * Groups failures by likely component.
   */
  public Map<String, List<FailureReport>> groupByComponent() {
    Map<String, List<FailureReport>> groups = new LinkedHashMap<>();
    for (FailureReport f: failures) {
      groups.computeIfAbsent(f.getLikelyComponent(), k -> new ArrayList<>()).add(f);
    }
    return groups;
  }

  /**
   * Groups failures by dimension pair (2-way combinations).
   */
  public Map<String, List<FailureReport>> groupByDimensionPair() {
    Map<String, List<FailureReport>> groups = new LinkedHashMap<>();
    for (FailureReport f: failures) {
      List<Map.Entry<DimensionId, String>> entries = new ArrayList<>(f.getDimensions().entrySet());
      for (int i = 0; i < entries.size(); i++) {
        for (int j = i + 1; j < entries.size(); j++) {
          String key = entries.get(i).getKey().name() + "=" + entries.get(i).getValue() + " + "
              + entries.get(j).getKey().name() + "=" + entries.get(j).getValue();
          groups.computeIfAbsent(key, k -> new ArrayList<>()).add(f);
        }
      }
    }
    return groups;
  }

  /**
   * Generates the HTML report at the specified directory.
   */
  public void generateHtmlReport(File reportDir) throws IOException {
    reportDir.mkdirs();
    File htmlFile = new File(reportDir, "feature-matrix-report.html");
    LOGGER.info("Generating HTML report at {}", htmlFile.getAbsolutePath());

    try (PrintWriter pw = new PrintWriter(new FileWriter(htmlFile))) {
      pw.println("<!DOCTYPE html>");
      pw.println("<html><head><title>Venice Feature Matrix Test Report</title>");
      pw.println("<style>");
      pw.println("body { font-family: sans-serif; margin: 20px; }");
      pw.println("table { border-collapse: collapse; margin: 10px 0; }");
      pw.println("th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }");
      pw.println("th { background-color: #4a90d9; color: white; }");
      pw.println(".pass { color: green; } .fail { color: red; } .skip { color: orange; }");
      pw.println("h2 { color: #333; border-bottom: 2px solid #4a90d9; padding-bottom: 5px; }");
      pw.println("</style></head><body>");

      // Summary
      pw.println("<h1>Venice Feature Matrix Test Report</h1>");
      pw.printf("<p>Total: %d | <span class='pass'>Passed: %d</span> | ", totalTestCount, passedCount);
      pw.printf(
          "<span class='fail'>Failed: %d</span> | <span class='skip'>Skipped: %d</span></p>%n",
          failedCount,
          skippedCount);

      if (failures.isEmpty()) {
        pw.println("<p class='pass'>All tests passed!</p>");
      } else {
        // By Component
        pw.println("<h2>Failures by Component</h2>");
        writeTable(pw, groupByComponent());

        // By Dimension (top 20 by failure count)
        pw.println("<h2>Failures by Dimension Value (Top 20)</h2>");
        Map<String, List<FailureReport>> byDim = groupByDimension();
        Map<String, List<FailureReport>> topDims = byDim.entrySet()
            .stream()
            .sorted((a, b) -> Integer.compare(b.getValue().size(), a.getValue().size()))
            .limit(20)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (a, b) -> a, LinkedHashMap::new));
        writeTable(pw, topDims);

        // By Dimension Pair (top 20 by failure count)
        pw.println("<h2>Failures by Dimension Pair (Top 20)</h2>");
        Map<String, List<FailureReport>> byPair = groupByDimensionPair();
        Map<String, List<FailureReport>> topPairs = byPair.entrySet()
            .stream()
            .sorted((a, b) -> Integer.compare(b.getValue().size(), a.getValue().size()))
            .limit(20)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (a, b) -> a, LinkedHashMap::new));
        writeTable(pw, topPairs);

        // Individual Failures
        pw.println("<h2>Individual Failures</h2>");
        pw.println("<table><tr><th>TC#</th><th>Step</th><th>Component</th><th>Error</th></tr>");
        for (FailureReport f: failures) {
          pw.printf(
              "<tr><td>%d</td><td>%s</td><td>%s</td><td>%s</td></tr>%n",
              f.getTestCaseId(),
              f.getValidationStep(),
              f.getLikelyComponent(),
              escapeHtml(f.getErrorMessage()));
        }
        pw.println("</table>");
      }

      pw.println("</body></html>");
    }

    LOGGER.info("HTML report generated: {}", htmlFile.getAbsolutePath());
  }

  /**
   * Generates structured JSON report for future regression tooling.
   */
  public void generateJsonReport(File reportDir) throws IOException {
    reportDir.mkdirs();
    File jsonFile = new File(reportDir, "feature-matrix-results.json");
    LOGGER.info("Generating JSON report at {}", jsonFile.getAbsolutePath());

    try (PrintWriter pw = new PrintWriter(new FileWriter(jsonFile))) {
      pw.println("{");
      pw.printf("  \"total\": %d,%n", totalTestCount);
      pw.printf("  \"passed\": %d,%n", passedCount);
      pw.printf("  \"failed\": %d,%n", failedCount);
      pw.printf("  \"skipped\": %d,%n", skippedCount);
      pw.println("  \"failures\": [");

      for (int i = 0; i < failures.size(); i++) {
        FailureReport f = failures.get(i);
        pw.println("    {");
        pw.printf("      \"testCaseId\": %d,%n", f.getTestCaseId());
        pw.printf("      \"validationStep\": \"%s\",%n", escapeJson(f.getValidationStep()));
        pw.printf("      \"component\": \"%s\",%n", escapeJson(f.getLikelyComponent()));
        pw.printf("      \"error\": \"%s\",%n", escapeJson(f.getErrorMessage()));
        pw.printf("      \"durationMs\": %d,%n", f.getDurationMs());
        pw.println("      \"dimensions\": {");
        int dimIdx = 0;
        for (Map.Entry<DimensionId, String> entry: f.getDimensions().entrySet()) {
          pw.printf(
              "        \"%s\": \"%s\"%s%n",
              entry.getKey().name(),
              escapeJson(entry.getValue()),
              dimIdx < f.getDimensions().size() - 1 ? "," : "");
          dimIdx++;
        }
        pw.println("      }");
        pw.printf("    }%s%n", i < failures.size() - 1 ? "," : "");
      }

      pw.println("  ]");
      pw.println("}");
    }

    LOGGER.info("JSON report generated: {}", jsonFile.getAbsolutePath());
  }

  private void writeTable(PrintWriter pw, Map<String, List<FailureReport>> groups) {
    pw.println("<table><tr><th>Group</th><th>Failure Count</th></tr>");
    for (Map.Entry<String, List<FailureReport>> entry: groups.entrySet()) {
      pw.printf("<tr><td>%s</td><td>%d</td></tr>%n", escapeHtml(entry.getKey()), entry.getValue().size());
    }
    pw.println("</table>");
  }

  private static String escapeHtml(String s) {
    if (s == null) {
      return "";
    }
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;");
  }

  private static String escapeJson(String s) {
    if (s == null) {
      return "";
    }
    return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r");
  }
}
