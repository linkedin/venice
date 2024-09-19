package com.linkedin.davinci.blobtransfer;

public class BlobTransferUtils {
  public static final String BLOB_TRANSFER_STATUS = "X-Blob-Transfer-Status";
  public static final String BLOB_TRANSFER_COMPLETED = "Completed";
  public static final String BLOB_TRANSFER_TYPE = "X-Blob-Transfer-Type";

  public enum BlobTransferType {
    FILE, METADATA
  }
}
