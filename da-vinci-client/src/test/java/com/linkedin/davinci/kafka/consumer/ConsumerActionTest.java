package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.kafka.consumer.ConsumerAction;
import com.linkedin.davinci.kafka.consumer.ConsumerActionType;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;

import org.testng.Assert;
import org.testng.annotations.Test;


public class ConsumerActionTest {
  @Test
  public void testQueueReturnActionsWithSamePriorityInCorrectOrder() {
    PriorityBlockingQueue<ConsumerAction> consumerActionsQueue = new PriorityBlockingQueue<>(10);
    String topic = "TestTopic";
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

      Assert.assertTrue(actualActionInQueue.equals(expectedAction),
          "ConsumerActionQueue return actions with the same priority in wrong order.");
    }
  }

  @Test
  public void testQueueReturnActionsWithDifferentPriorityInCorrectOrder() {
    PriorityBlockingQueue<ConsumerAction> consumerActionsQueue = new PriorityBlockingQueue<>(10);
    String topic = "TestTopic";
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
          Assert.fail("For actions that share the same priority, "
              + "an action with smaller sequence number somehow ends up at the back of the queue.");
        }
      }
    }
  }

  private ConsumerAction getRandomAction(String topic, int partition, int sequenceNumber, int priority) {
    ConsumerActionType[] actionTypes = ConsumerActionType.values();
    while (true) {
      int index = ThreadLocalRandom.current().nextInt(actionTypes.length);
      if (actionTypes[index].getActionPriority() == priority) {
        return new ConsumerAction(actionTypes[index], topic, partition, sequenceNumber);
      }
    }
  }
}
