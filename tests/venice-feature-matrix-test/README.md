# Venice Feature Matrix Integration Test

3-way combinatorial integration test covering 42 feature dimensions across Venice components.

## Overview

This module uses PICT (Pairwise Independent Combinatorial Testing) to generate test cases that cover all 3-way
interactions between 42 feature dimensions spanning write path, read path, server, controller, and router
configurations. This systematic approach catches interaction bugs that isolated per-feature tests miss.

## Prerequisites

- Java 8+
- Gradle (provided by wrapper)
- **PICT tool** (for regenerating test cases): [github.com/microsoft/pict](https://github.com/microsoft/pict)
  - macOS: `brew install pict`
  - Linux: build from source

## How to Run

```bash
# Run all feature matrix tests
./gradlew :tests:venice-feature-matrix-test:featureMatrixTest

# Build only (compile check)
./gradlew :tests:venice-feature-matrix-test:build
```

Reports are generated at:

- `tests/venice-feature-matrix-test/build/reports/feature-matrix-report.html`
- `tests/venice-feature-matrix-test/build/reports/feature-matrix-results.json`

## How to Add a New Feature Flag Dimension

1. **Add dimension to PICT model**: Edit `src/test/resources/feature-matrix-model.pict`

   ```
   NEW_DIMENSION:  value1, value2, value3
   ```

2. **Add constraints** (if any):

   ```
   IF [NEW_DIMENSION] = "value1" THEN [EXISTING_DIM] = "on";
   ```

3. **Regenerate test cases**:

   ```bash
   pict src/test/resources/feature-matrix-model.pict /o:3 > src/test/resources/generated-test-cases.tsv
   ```

4. **Update Java model**:

   - Add enum value to `FeatureDimensions.DimensionId`
   - Add field and getter to `TestCaseConfig`

5. **Add handling in setup classes**:

   - Store-level flags: `StoreConfigurator`
   - Server/Router/Controller flags: `ClusterConfigBuilder`
   - Client flags: `ClientFactory`

6. **Add validation** (if needed): Update validators in `validation/` package

## How to Read the Report

### HTML Report (`feature-matrix-report.html`)

- **Failures by Component**: Shows which Venice component is most affected
  - e.g., "Server (ingestion): 15 failures" suggests server-side bugs
- **Failures by Dimension Value**: Shows which flag values correlate with failures
  - e.g., "W4_WC=on: 23 failures, W4_WC=off: 0" suggests Write Compute has a bug
- **Failures by Dimension Pair**: Shows interaction bugs between two flags
  - e.g., "(W4_WC=on, W8_ZSTD): 8 failures" suggests WC+ZSTD interaction bug

### JSON Report (`feature-matrix-results.json`)

Structured output for automated tooling. Each failure includes the full 42-dimensional context, validation step,
classified component, and exception details.

## Architecture

```
src/test/java/com/linkedin/venice/featurematrix/
|-- FeatureMatrixIntegrationTest.java  # Main test (Factory + DataProvider)
|-- FeatureMatrixClusterSetup.java     # Cluster lifecycle
|-- model/
|   |-- FeatureDimensions.java         # 42 dimension enums
|   |-- TestCaseConfig.java            # Single test case config
|   |-- PictModelParser.java           # Parses PICT output
|-- setup/
|   |-- StoreConfigurator.java         # Store creation with W flags
|   |-- ClusterConfigBuilder.java      # Cluster props from S/RT/C dims
|   |-- ClientFactory.java             # Creates Thin/Fast/DaVinci clients
|-- write/
|   |-- BatchPushExecutor.java         # VPJ batch push
|   |-- StreamingWriteExecutor.java    # VeniceWriter RT writes
|   |-- IncrementalPushExecutor.java   # Incremental push
|-- validation/
|   |-- DataIntegrityValidator.java    # Single/batch get correctness
|   |-- ReadComputeValidator.java      # Read compute validation
|   |-- WriteComputeValidator.java     # Partial update validation
|-- reporting/
    |-- FeatureMatrixReportListener.java   # TestNG listener
    |-- FeatureMatrixReportAggregator.java # Groups failures
    |-- FailureReport.java                 # Per-failure data

src/test/resources/
|-- feature-matrix-model.pict          # PICT model (42 dims + constraints)
|-- generated-test-cases.tsv           # PICT output (regenerate as needed)
```

## For AI Agents

When maintaining this test module:

- The PICT model at `src/test/resources/feature-matrix-model.pict` is the source of truth for dimensions and constraints
- `TestCaseConfig` must have a field/getter for every dimension in `DimensionId`
- When adding a new dimension, update all four places: PICT model, DimensionId enum, TestCaseConfig, and the appropriate
  setup class
- Cluster configs (S, RT, C dimensions) require cluster restart; store configs (W dimensions) can change per-test
- See `DESIGN.md` for full architectural details and dependency chains
