package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.davinci.kafka.consumer.LeaderFollowerStateType.LEADER;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;

import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ConsumerActionTest {
  private final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

  @Test
  public void testQueueReturnActionsWithSamePriorityInCorrectOrder() {
    PriorityBlockingQueue<ConsumerAction> consumerActionsQueue = new PriorityBlockingQueue<>(10);
    String topic = "TestTopic_v1";
    int sequenceNumber = 0;
    int actionsNumber = 100;
    List<ConsumerAction> consumerActionList = new LinkedList<>();
    for (int i = 0; i < actionsNumber; i++) {
      ConsumerAction action = getRandomAction(topic, 0, ++sequenceNumber, 1);
      consumerActionList.add(action);
      consumerActionsQueue.add(action);
    }
    Iterator<ConsumerAction> iter = consumerActionList.iterator();
    while (!consumerActionsQueue.isEmpty()) {
      ConsumerAction actualActionInQueue = consumerActionsQueue.poll();

      if (!iter.hasNext()) {
        Assert.fail("The same consumer action has been added to action queue more than one time.");
      }
      ConsumerAction expectedAction = iter.next();

      Assert.assertTrue(
          actualActionInQueue.equals(expectedAction),
          "ConsumerActionQueue return actions with the same priority in wrong order.");
    }
  }

  @Test
  public void testQueueReturnActionsWithDifferentPriorityInCorrectOrder() {
    PriorityBlockingQueue<ConsumerAction> consumerActionsQueue = new PriorityBlockingQueue<>(10);
    String topic = "TestTopic_v1";
    int sequenceNumber = 0;
    int actionsNumber = 100;
    int MAX_PRIORITY = 2;
    for (int i = 0; i < actionsNumber; i++) {
      ConsumerAction action = getRandomAction(topic, 0, ++sequenceNumber, i % MAX_PRIORITY + 1);
      consumerActionsQueue.add(action);
    }

    ConsumerAction firstAction = consumerActionsQueue.poll();
    int previousPriority = firstAction.getType().getActionPriority();
    int previousSequenceNumber = firstAction.getSequenceNumber();
    while (!consumerActionsQueue.isEmpty()) {
      ConsumerAction action = consumerActionsQueue.poll();
      if (action.getType().getActionPriority() > previousPriority) {
        Assert.fail("Consumer action with high priority is not moved to the head of the queue.");
      } else if (action.getType().getActionPriority() == previousPriority) {
        if (action.getSequenceNumber() <= previousSequenceNumber) {
          Assert.fail(
              "For actions that share the same priority, "
                  + "an action with smaller sequence number somehow ends up at the back of the queue.");
        }
      }
    }
  }

  @Test
  public void testEqualsAndHashCode() {
    PubSubTopicPartition pubSubTopicPartition1 =
        new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic("blah_rt"), 0);
    ConsumerAction ca1 = new ConsumerAction(ConsumerActionType.RESUME, pubSubTopicPartition1, 0);

    PubSubTopicPartition pubSubTopicPartition2 =
        new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic("blah_rt"), 1);
    ConsumerAction ca2 = new ConsumerAction(ConsumerActionType.RESUME, pubSubTopicPartition2, 0);

    assertNotEquals(ca1, ca2);
    assertNotEquals(ca1.hashCode(), ca2.hashCode());
    assertFalse(ca1.equals(null));

    PubSubTopicPartition pubSubTopicPartition3 =
        new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic("blah_rt"), 1);
    ConsumerAction ca3 = new ConsumerAction(ConsumerActionType.RESUME, pubSubTopicPartition3, 0);

    assertEquals(ca2, ca3);
    assertEquals(ca2.hashCode(), ca3.hashCode());

    ConsumerAction ca4 = new ConsumerAction(ConsumerActionType.RESUME, pubSubTopicPartition3, 1);
    assertNotEquals(ca2, ca4);
    assertNotEquals(ca3, ca4);

    ConsumerAction ca5 = new ConsumerAction(ConsumerActionType.KILL, pubSubTopicPartition3, 0);
    assertNotEquals(ca2, ca5);
    assertNotEquals(ca3, ca5);
    assertNotEquals(ca4, ca5);

    ConsumerAction ca6 = new ConsumerAction(ConsumerActionType.KILL, pubSubTopicPartition3, 0, Optional.of(LEADER));
    assertNotEquals(ca2, ca6);
    assertNotEquals(ca3, ca6);
    assertNotEquals(ca4, ca6);
    assertNotEquals(ca5, ca6);
  }

  private ConsumerAction getRandomAction(String topic, int partition, int sequenceNumber, int priority) {
    ConsumerActionType[] actionTypes = ConsumerActionType.values();
    while (true) {
      int index = ThreadLocalRandom.current().nextInt(actionTypes.length);
      if (actionTypes[index].getActionPriority() == priority) {
        return new ConsumerAction(
            actionTypes[index],
            new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic(topic), partition),
            sequenceNumber);
      }
    }
  }
}
