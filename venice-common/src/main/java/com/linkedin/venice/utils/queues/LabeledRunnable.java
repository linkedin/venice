package com.linkedin.venice.utils.queues;

public class LabeledRunnable implements Labeled, Runnable {
  private String label;
  private Runnable runnable;

  public LabeledRunnable(String label, Runnable runnable){
    this.label = label;
    this.runnable = runnable;
  }

  @Override
  public void run() {
    runnable.run();
  }

  @Override
  public String getLabel(){
    return label;
  }

}
