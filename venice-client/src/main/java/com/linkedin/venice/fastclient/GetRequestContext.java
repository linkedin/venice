package com.linkedin.venice.fastclient;

public class GetRequestContext extends RequestContext {
  int partitionId = -1;
  boolean noAvailableReplica = false;

  double decompressionTime = -1;
  double responseDeserializationTime = -1;
  double requestSerializationTime = -1;
  double requestSubmissionToResponseHandlingTime = -1;

}
