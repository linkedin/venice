package com.linkedin.venice.helix;

import com.linkedin.venice.job.ExecutionStatus;
import com.linkedin.venice.job.Task;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Test cases for TaskJSONSerializer.
 */
public class TestTasksJSONSerializer {
  @Test
  public void testSerializeAndDeserializeTask()
      throws IOException {
    TasksJSONSerializer serializer = new TasksJSONSerializer();

    List<Task> tasks = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      Task task = new Task(String.valueOf(i), i, "instance" + i, ExecutionStatus.ERROR);
      tasks.add(task);
    }

    byte[] data = serializer.serialize(tasks);

    List<Task> newTasks = serializer.deserialize(data);
    Assert.assertEquals(newTasks.size(), 10);
    for (int i = 0; i < 10; i++) {
      Assert.assertEquals(newTasks.get(i).getTaskId(), String.valueOf(i));
      Assert.assertEquals(newTasks.get(i).getPartitionId(), i);
      Assert.assertEquals(newTasks.get(i).getInstanceId(), "instance" + i);
      Assert.assertEquals(newTasks.get(i).getStatus(), ExecutionStatus.ERROR);
    }
  }
}
