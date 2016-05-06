package com.linkedin.venice.controller.kafka;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.service.AbstractVeniceService;


/**
 * Created by mwise on 5/6/16.
 */
public class TopicMonitor extends AbstractVeniceService {

  private Admin admin;
  private TopicMonitorRunnable monitor;
  private Thread runner;


  public TopicMonitor(Admin admin) {
    super("TOPIC-MONITOR-SERVICE");
    this.admin = admin;
  }

  @Override
  public void startInner() throws Exception {
    String kafkaString = admin.getKafkaBootstrapServers();
    monitor = new TopicMonitorRunnable(admin);
    runner = new Thread(monitor);
    runner.setName("TopicMonitorThread - " + kafkaString);
    runner.setDaemon(true);
    runner.start();
  }

  @Override
  public void stopInner() throws Exception {
    monitor.setStop();
    runner.interrupt();
  }

  private class TopicMonitorRunnable implements Runnable {

    private volatile boolean stop = false;
    private Admin admin;

    TopicMonitorRunnable(Admin admin){
      this.admin = admin;
    }

    protected void setStop(){
      stop = true;
    }

    @Override
    public void run() {
      while(true){
        try{
          Thread.sleep(1000);
          System.out.println("One second has passed");
          //do stuff
        } catch (InterruptedException e){
          if (stop){
            System.out.println("Stopping!");
            break;
          }
        }
      }
      System.out.println("finished.");
    }
  }
}
