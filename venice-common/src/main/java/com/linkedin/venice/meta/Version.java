package com.linkedin.venice.meta;

import com.sun.istack.internal.NotNull;
import java.io.Serializable;

import static com.linkedin.venice.meta.MetadataConstants.*;


/**
 * Class defines the version of Venice store.
 */
public class Version implements Serializable {
    /**
     * Version number.
     */
    private final int number;
    /**
     * Time when this version was created.
     */
    private final long createdTime;
    /**
     * Kafka topic used by this version.
     */
    private String kafkaTopic = "";
    /**
     * Status of version.
     */
    private String status = INACTIVE;

    public Version(int number, long createdTime) {
        this.number = number;
        this.createdTime = createdTime;
    }

    public int getNumber() {
        return number;
    }

    public long getCreatedTime() {
        return createdTime;
    }

    public String getKafkaTopic() {
        return kafkaTopic;
    }

    public void setKafkaTopic(@NotNull String kafkaTopic) {
        this.kafkaTopic = kafkaTopic;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(@NotNull String status) {
        this.status = status;
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
        if (!kafkaTopic.equals(version.kafkaTopic)) {
            return false;
        }
        return status.equals(version.status);
    }

    @Override
    public int hashCode() {
        int result = number;
        result = 31 * result + (int) (createdTime ^ (createdTime >>> 32));
        result = 31 * result + kafkaTopic.hashCode();
        result = 31 * result + status.hashCode();
        return result;
    }

    /**
     * Clone a new version based on current data in this version.
     *
     * @return cloned version.
     */
    public Version cloneVersion() {
        Version clonedVersion = new Version(number, createdTime);
        clonedVersion.setKafkaTopic(kafkaTopic);
        clonedVersion.setStatus(status);
        return clonedVersion;
    }
}
