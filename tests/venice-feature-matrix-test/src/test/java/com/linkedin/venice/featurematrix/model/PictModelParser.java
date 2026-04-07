package com.linkedin.venice.featurematrix.model;

import com.linkedin.venice.featurematrix.model.FeatureDimensions.DimensionId;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Parses PICT-generated tab-separated test case files into {@link TestCaseConfig} objects.
 *
 * Expected format:
 * - First line: tab-separated column headers matching PICT dimension names
 * - Subsequent lines: tab-separated values for each test case
 */
public class PictModelParser {
  private static final Logger LOGGER = LogManager.getLogger(PictModelParser.class);

  /**
   * Parses a PICT output file from the classpath.
   *
   * @param resourcePath classpath resource path (e.g., "generated-test-cases.tsv")
   * @return list of parsed test case configurations
   */
  public static List<TestCaseConfig> parseFromClasspath(String resourcePath) throws IOException {
    try (InputStream is = PictModelParser.class.getClassLoader().getResourceAsStream(resourcePath)) {
      if (is == null) {
        throw new IOException("Resource not found: " + resourcePath);
      }
      return parse(new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8)));
    }
  }

  /**
   * Parses a PICT output file from a filesystem path.
   */
  public static List<TestCaseConfig> parseFromFile(Path filePath) throws IOException {
    try (BufferedReader reader = Files.newBufferedReader(filePath, StandardCharsets.UTF_8)) {
      return parse(reader);
    }
  }

  private static List<TestCaseConfig> parse(BufferedReader reader) throws IOException {
    String headerLine = reader.readLine();
    if (headerLine == null) {
      throw new IOException("Empty PICT file: no header line");
    }

    String[] headers = headerLine.split("\t");
    DimensionId[] columnDimensions = mapHeadersToDimensions(headers);

    List<TestCaseConfig> testCases = new ArrayList<>();
    String line;
    int lineNumber = 1;

    while ((line = reader.readLine()) != null) {
      lineNumber++;
      line = line.trim();
      if (line.isEmpty()) {
        continue;
      }

      String[] values = line.split("\t");
      if (values.length != headers.length) {
        LOGGER.warn("Line {} has {} values but expected {}, skipping", lineNumber, values.length, headers.length);
        continue;
      }

      Map<DimensionId, String> dimensions = new LinkedHashMap<>();
      for (int i = 0; i < values.length; i++) {
        if (columnDimensions[i] != null) {
          dimensions.put(columnDimensions[i], values[i].trim());
        }
      }

      testCases.add(new TestCaseConfig(testCases.size() + 1, dimensions));
    }

    LOGGER.info("Parsed {} test cases from PICT output", testCases.size());
    return testCases;
  }

  /**
   * Maps PICT column headers to DimensionId enum values.
   */
  private static DimensionId[] mapHeadersToDimensions(String[] headers) {
    DimensionId[] result = new DimensionId[headers.length];

    // Build a lookup from PICT column name to DimensionId
    Map<String, DimensionId> lookup = new LinkedHashMap<>();
    for (DimensionId dim: DimensionId.values()) {
      lookup.put(dim.getPictColumnName(), dim);
    }

    for (int i = 0; i < headers.length; i++) {
      String header = headers[i].trim();
      result[i] = lookup.get(header);
      if (result[i] == null) {
        LOGGER.warn("Unknown PICT column header: '{}', will be ignored", header);
      }
    }

    return result;
  }

  /**
   * Groups test cases by their unique cluster config key (S,RT,C tuple).
   */
  public static Map<String, List<TestCaseConfig>> groupByClusterConfig(List<TestCaseConfig> testCases) {
    Map<String, List<TestCaseConfig>> groups = new LinkedHashMap<>();
    for (TestCaseConfig tc: testCases) {
      groups.computeIfAbsent(tc.getClusterConfigKey(), k -> new ArrayList<>()).add(tc);
    }
    LOGGER.info("Grouped {} test cases into {} unique cluster configurations", testCases.size(), groups.size());
    return groups;
  }
}
