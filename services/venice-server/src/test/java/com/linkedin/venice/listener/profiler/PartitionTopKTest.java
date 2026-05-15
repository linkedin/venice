package com.linkedin.venice.listener.profiler;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.List;
import org.testng.annotations.Test;


public class PartitionTopKTest {
  @Test
  public void respectsCapacity() {
    PartitionTopK heap = new PartitionTopK(3);
    for (int i = 0; i < 10; i++) {
      heap.update(("k" + i).getBytes(StandardCharsets.UTF_8), i + 1L);
    }
    assertEquals(heap.size(), 3);
  }

  @Test
  public void retainsHottestKeysAcrossInserts() {
    PartitionTopK heap = new PartitionTopK(3);
    heap.update("hot".getBytes(StandardCharsets.UTF_8), 1000);
    heap.update("warm".getBytes(StandardCharsets.UTF_8), 500);
    heap.update("mild".getBytes(StandardCharsets.UTF_8), 100);
    // These five should each lose to "mild".
    heap.update("cold-a".getBytes(StandardCharsets.UTF_8), 50);
    heap.update("cold-b".getBytes(StandardCharsets.UTF_8), 60);
    heap.update("cold-c".getBytes(StandardCharsets.UTF_8), 70);
    heap.update("cold-d".getBytes(StandardCharsets.UTF_8), 80);
    heap.update("cold-e".getBytes(StandardCharsets.UTF_8), 90);

    List<PartitionTopK.Entry> snapshot = heap.snapshotSortedDescending();
    assertEquals(snapshot.size(), 3);
    assertEquals(new String(snapshot.get(0).keyHash, StandardCharsets.UTF_8), "hot");
    assertEquals(snapshot.get(0).count, 1000);
    assertEquals(new String(snapshot.get(1).keyHash, StandardCharsets.UTF_8), "warm");
    assertEquals(new String(snapshot.get(2).keyHash, StandardCharsets.UTF_8), "mild");
  }

  @Test
  public void repeatedUpdatesBumpExistingEntry() {
    PartitionTopK heap = new PartitionTopK(3);
    byte[] hot = "h".getBytes(StandardCharsets.UTF_8);
    for (int i = 1; i <= 1000; i++) {
      heap.update(hot, i);
    }
    heap.update("other".getBytes(StandardCharsets.UTF_8), 5);
    List<PartitionTopK.Entry> snapshot = heap.snapshotSortedDescending();
    assertEquals(snapshot.get(0).count, 1000);
    assertEquals(heap.size(), 2);
  }

  @Test
  public void newKeyAtCapacityEvictsMinOnlyWhenStrictlyGreater() {
    PartitionTopK heap = new PartitionTopK(2);
    heap.update("a".getBytes(), 10);
    heap.update("b".getBytes(), 20);
    // 'c' ties with the min — should NOT evict.
    heap.update("c".getBytes(), 10);
    List<PartitionTopK.Entry> snapshot = heap.snapshotSortedDescending();
    assertEquals(snapshot.size(), 2);
    assertTrue(snapshot.stream().anyMatch(e -> new String(e.keyHash).equals("a")));
    assertTrue(snapshot.stream().anyMatch(e -> new String(e.keyHash).equals("b")));
  }

  @Test
  public void belowCapacityAcceptsAll() {
    PartitionTopK heap = new PartitionTopK(5);
    heap.update("x".getBytes(), 1);
    heap.update("y".getBytes(), 2);
    assertEquals(heap.size(), 2);
  }
}
