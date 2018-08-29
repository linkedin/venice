package com.linkedin.venice.utils.queues;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.commons.math3.stat.StatUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestFairBlockingQueue {

  @Test
  public void fairQueueIsFair(){
    BlockingQueue<Runnable> queue = new FairBlockingQueue();
    //Add 10 red items
    for (int i=0; i<10; i++){
      queue.add(new LabeledRunnable("red", ()->{}));
    }
    //Then add 10 blue items
    for (int i=0; i<10; i++){
      queue.add(new LabeledRunnable("blue", ()->{}));
    }
    //Then add a green item
    queue.add(new LabeledRunnable("green", ()->{}));

    //Removing should yield interleaved order, with green item (added at spot 21) in third spot
    for (int i=0; i<10; i++) {
      LabeledRunnable r1 = (LabeledRunnable) queue.remove();
      Assert.assertEquals(r1.getLabel(), "red");
      LabeledRunnable r2 = (LabeledRunnable) queue.remove();
      Assert.assertEquals(r2.getLabel(), "blue");
      if (i==0){
        LabeledRunnable r3 = (LabeledRunnable) queue.remove();
        Assert.assertEquals(r3.getLabel(), "green");
      }
    }
  }

  //This test lets us compare the FairBlockingQueue's performance to that of a LinkedBlockingQueue.
  // 4/16/2018 - 100 threads each add 10k items and then 10 threads consume those million items
  //   LinkedBlockingQueue ~ 250ms (0.25us per item)
  //   FairBlockingQueue   ~ 500ms (0.5us per item)
  @Test(enabled = false)
  public void linkedVsFairPerformance() throws InterruptedException {
    int testCount = 10;

    double[] fairData = new double[testCount];
    for (int i=0; i<testCount; i++){
      fairData[i] = testThroughput(new FairBlockingQueue());
    }

    double[] linkedData = new double[testCount];
    for (int i=0; i<testCount; i++){
      linkedData[i] = testThroughput(new LinkedBlockingQueue<>());
    }

    System.out.println("linked mean: " + StatUtils.mean(linkedData));
    System.out.println("fair mean: " + StatUtils.mean(fairData));

  }

  public double testThroughput(BlockingQueue<Runnable> queue) throws InterruptedException {
    String[] categories = new String[]{"red", "green", "blue", "yellow"};

    Runnable addToQueue = () -> {
      Random random = new Random();
      for (int i=0; i<10000; i++) {
        String category = categories[random.nextInt(categories.length)];
        queue.add(new LabeledRunnable(category, ()-> {
          return;
        }));
      }
    };

    Runnable removeFromQueue = () -> {
      Runnable r = queue.poll();
      while (r != null){
        r.run();
        r = queue.poll();
      }

    };

    long start = System.currentTimeMillis();

    //Add to queue threads.
    List<Thread> threads = new ArrayList<>();
    for (int i=0; i<100; i++){
      Thread t = new Thread(addToQueue);
      t.start();
      threads.add(t);
    }
    for (Thread t : threads){
      t.join();
    }

    //Remove from queue threads
    for (int i=0; i<10; i++){
      Thread t = new Thread(removeFromQueue);
      t.start();
      threads.add(t);
    }

    for (Thread t : threads){
      t.join();
    }

    long end = System.currentTimeMillis();
    long duration = (end - start);
    System.out.println("Duration: " + duration);
    System.out.println("Queue Size: " + queue.size());
    return duration;
  }
}
