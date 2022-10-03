package com.linkedin.alpini.consts;

/**
 * Sets the quality of service (QOS) for the request. A request with a high QOS will be serviced preferentially
 * over a low QOS. Assigning backend and batch jobs a low QOS will help keep high priority frontend requests responsive,
 * even when these batch jobs are running.
 *
 * @author Jemiah Westerman &lt;jwesterman@linkedin.com&gt;
 */
public enum QOS {
  LOW, NORMAL, HIGH
}
