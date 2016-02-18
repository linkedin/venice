package com.linkedin.venice.meta;

import javax.validation.constraints.NotNull;


/**
 * Class defines the version of Venice store.
 */
public class Version{
    /**
     * Name of the store which this version belong to.
     */
    private final String storeName;
    /**
     * Version number.
     */
    private final int number;
    /**
     * Time when this version was created.
     */
    private final long createdTime;
    /**
     * Status of version.
     */
    private VersionStatus status = VersionStatus.ACTIVE;

    public Version(@NotNull String storeName, int number, long createdTime) {
        this.storeName = storeName;
        this.number = number;
        this.createdTime = createdTime;
    }

    public int getNumber() {
        return number;
    }

    public long getCreatedTime() {
        return createdTime;
    }

    public VersionStatus getStatus() {
        return status;
    }

    public void setStatus(@NotNull VersionStatus status) {
        this.status = status;
    }

    public String getStoreName() {
        return storeName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Version version = (Version) o;

        if (number != version.number) {
            return false;
        }
        if (createdTime != version.createdTime) {
            return false;
        }
        if (!storeName.equals(version.storeName)) {
            return false;
        }
        return status.equals(version.status);
    }

    @Override
    public int hashCode() {
        int result = storeName.hashCode();
        result = 31 * result + number;
        result = 31 * result + (int) (createdTime ^ (createdTime >>> 32));
        result = 31 * result + status.hashCode();
        return result;
    }

    /**
     * Clone a new version based on current data in this version.
     *
     * @return cloned version.
     */
    public Version cloneVersion() {
        Version clonedVersion = new Version(storeName, number, createdTime);
        clonedVersion.setStatus(status);
        return clonedVersion;
    }

    /**
     * Kafka topic name is composed by store name and version.
     *
     * @return kafka topic name.
     */
    public String getKafkaTopicName() {
        return storeName + number;
    }
}
