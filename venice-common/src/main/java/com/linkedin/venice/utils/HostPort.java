package com.linkedin.venice.utils;

/**
 *.
 */
public class HostPort {
  private String hostname;
  private String port;

  public HostPort(String hostname, String port){
    this.hostname = hostname;
    this.port = port;
  }

  public String getPort() {
    return port;
  }

  public void setPort(String port) {
    this.port = port;
  }

  public String getHostname() {
    return hostname;
  }

  public void setHostname(String hostname) {
    this.hostname = hostname;
  }

  @Override
  public String toString(){
    return "Host: " + hostname + ", Port: " + port;
  }

}
