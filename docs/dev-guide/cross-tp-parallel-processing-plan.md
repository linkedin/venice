# Cross-TP Parallel Processing

Parallelizes processing of topic-partitions within `ConsumptionTask` to prevent slow TPs from blocking others in the same poll batch.

## Configuration

```properties
server.cross.tp.parallel.processing.enabled=true
server.cross.tp.parallel.processing.thread.pool.size=4  # default
```

## Problem

Without this feature, TPs are processed **sequentially** in `ConsumptionTask`. A slow TP blocks all others in the same poll batch, inflating `leader_preprocessing_latency`.

## Solution

Submit each TP's `write()` call to a shared thread pool using `CompletableFuture.supplyAsync()`. Falls back to sequential processing when:
- Pool is null (feature disabled)
- Only 1 TP in poll batch

## Architecture

```
AggKafkaConsumerService (creates single shared crossTpProcessingPool)
    └── KafkaConsumerService (receives pool via constructor)
            └── ConsumptionTask (uses pool for parallel TP processing)
```

**Key points:**
- Single shared thread pool across all `KafkaConsumerService` instances
- Pool shutdown handled in `AggKafkaConsumerService.stopInner()`
- `TpProcessingResult` class aggregates results from parallel processing

## Files Modified

| File | Change |
|------|--------|
| `ConfigKeys.java` | Config keys |
| `VeniceServerConfig.java` | Config parsing and getters |
| `AggKafkaConsumerService.java` | Creates/manages shared thread pool |
| `KafkaConsumerService.java` | Receives pool, passes to ConsumptionTask |
| `PartitionWiseKafkaConsumerService.java` | Constructor updated |
| `StoreAwarePartitionWiseKafkaConsumerService.java` | Constructor updated |
| `ConsumptionTask.java` | Parallel processing logic |

## Testing

- `ConsumptionTaskTest.java` - 4 unit tests (parallel, sequential, single TP, missing receiver)
- `TestActiveActiveIngestionWithCrossTpParallelProcessing.java` - Integration test

## Constraints Preserved

- **Within-TP ordering** - Entire TP submitted as single task
- **Same-key ordering** - Handled by existing `IngestionBatchProcessor`
- **Error handling** - Errors collected and logged per TP
