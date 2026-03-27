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

## For AI Agents: Adding a New Feature Flag

When a user tells you about a new Venice configuration they are adding, follow ALL steps below in order.
The user will provide: the flag name, component (server/controller/router/write/read), type, default value,
values to test, maturity level, and any dependencies or constraints.

### Step 1: Add catalog entry to `feature-flag-catalog.md`

Append an entry to the appropriate section using this exact template:

```markdown
#### `CONFIG_KEY_NAME`

- **Source**: ConfigKeys.java (or the relevant config class)
- **Type**: boolean | int | string | enum
- **Default**: <default value>
- **Scope**: Store | Cluster | Version
- **Set By**: Operator | User | System
- **Affects**: Server | Controller | Router | Client
- **Path Impact**: Write | Read | Both
- **Maturity**: MATURE | MATURING | EXPERIMENTAL — <brief explanation>
- **Dependencies**: <other flags this depends on, or "None">
- **Dependents**: <flags that depend on this, or "None">
- **Description**: <what it does>
```

### Step 2: Add dimension to PICT model (`src/test/resources/feature-matrix-model.pict`)

Choose the correct component group prefix and next available number:
- Write path: `W<N>_<Name>` (under `# Write Path` section)
- Read path: `R<N>_<Name>` (under `# Read Path` section)
- Server: `S<N>_<Name>` (under `# Server` section)
- Controller: `C<N>_<Name>` (under `# Controller` section)
- Router: Currently excluded from PICT (see Router section comment)

For MATURE flags (always-on in production), fix to a single value:
```
# S<N>: MATURE — <reason>
S<N>_FlagName:    on
```

For MATURING/EXPERIMENTAL flags, list all test values:
```
S<N>_FlagName:    on, off
```

Update the dimension count comment on line 2 accordingly.

Add constraints under the `# Constraints` section if the new dimension has dependencies:
```
IF [W<N>_NewFlag] = "on" THEN [W<M>_Dependency] = "on";
```

### Step 3: Regenerate test cases

Run PICT to regenerate the TSV. Use the same order (`/o:N`) shown in the model's header comment:
```bash
pict src/test/resources/feature-matrix-model.pict /o:3 > src/test/resources/generated-test-cases.tsv
```

If `pict` is not installed, tell the user to install it (`brew install pict` on macOS) and run the command.

### Step 4: Add to `FeatureDimensions.java`

1. If the dimension needs a **new enum type** (not boolean on/off), add it in the appropriate component section:
   ```java
   public enum NewEnumType {
     VALUE_A, VALUE_B, VALUE_C
   }
   ```

2. Add to the `DimensionId` enum. The `pictColumnName` string MUST exactly match the PICT model column header:
   ```java
   // In the appropriate component group (Write/Read/Server/Controller/Router)
   S7_NEW_FLAG("S7_NewFlag"),
   ```

### Step 5: Add to `TestCaseConfig.java`

1. Add a **field** in the appropriate component section:
   ```java
   // Server
   private final boolean newFlag;  // for on/off dimensions
   // OR
   private final NewEnumType newFlag;  // for enum dimensions
   ```

2. Add **parsing** in the constructor, in the appropriate component section:
   ```java
   // For on/off:
   this.newFlag = isOn(DimensionId.S7_NEW_FLAG);
   // For enum:
   this.newFlag = parseNewEnumType(get(DimensionId.S7_NEW_FLAG));
   ```

3. Add a **getter**:
   ```java
   public boolean isNewFlag() {  // boolean
     return newFlag;
   }
   // OR
   public NewEnumType getNewFlag() {  // enum
     return newFlag;
   }
   ```

4. If the dimension uses a new enum type, add a **parse method**:
   ```java
   private static NewEnumType parseNewEnumType(String value) {
     switch (value) {
       case "value_a": return NewEnumType.VALUE_A;
       // ... one case per PICT value
       default: throw new IllegalArgumentException("Unknown NewEnumType: " + value);
     }
   }
   ```

### Step 6: Add handling in the appropriate setup/execution class

Choose based on WHERE the flag is applied:

| Flag scope | Class | How |
|---|---|---|
| Store-level (UpdateStoreQueryParams) | `StoreConfigurator.java` | Add to `buildUpdateParams()` |
| Server properties | `ClusterConfigBuilder.java` | Add to server properties map |
| Controller properties | `ClusterConfigBuilder.java` | Add to controller properties map |
| Router properties | `ClusterConfigBuilder.java` | Add to router properties map (currently not wired) |
| Client config | `ClientFactory.java` | Add to client builder |
| Push job config | `BatchPushExecutor.java` | Add to push job properties |
| Cluster setup | `FeatureMatrixClusterSetup.java` | Add to cluster create options |

Example for a server property in `ClusterConfigBuilder`:
```java
if (config.isNewFlag()) {
  serverProperties.put(ConfigKeys.NEW_FLAG_ENABLED, "true");
}
```

Example for a store-level flag in `StoreConfigurator`:
```java
params.setNewFlagEnabled(config.isNewFlag());
```

### Step 7: Add validation (if needed)

If the new flag changes read behavior, update the appropriate validator:
- `DataIntegrityValidator.java` — single/batch get correctness
- `ReadComputeValidator.java` — read compute operations
- `WriteComputeValidator.java` — partial update / write compute

### Important rules

- The PICT model column name, `DimensionId.pictColumnName`, and TSV header MUST all match exactly
- Cluster-level configs (S, C, RT prefixes) affect cluster grouping for sharded tests — adding a new
  varied S/C dimension increases the number of unique cluster configs and may increase shard count
- Store-level configs (W prefix) can vary per test case within a shared cluster
- MATURE flags fixed to a single value do NOT increase combinatorial space
- After all changes, verify compilation: `./gradlew :tests:venice-feature-matrix-test:compileTestJava`
- See `DESIGN.md` for full architectural details and dependency chains
