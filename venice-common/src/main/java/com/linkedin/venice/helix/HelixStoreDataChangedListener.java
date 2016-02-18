package com.linkedin.venice.helix;

import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreDataChangedListener;
import javax.validation.constraints.NotNull;
import org.I0Itec.zkclient.IZkDataListener;


/**
 * Listener handle the ZNode data changed event. In order to avoid exposing I0ITec API, this listener used as a bridge
 * to convert zookeeper event to Venice event.
 */
class HelixStoreDataChangedListener implements IZkDataListener {
    private final String storePath;
    private final StoreDataChangedListener listener;

    public HelixStoreDataChangedListener(@NotNull String rootPath, @NotNull String storeName,
        @NotNull StoreDataChangedListener listener) {
        storePath = rootPath + "/" + storeName;
        this.listener = listener;
    }

    @Override
    public void handleDataChange(String dataPath, Object data)
        throws Exception {

        if (!dataPath.equals(storePath)) {
            throw new IllegalArgumentException(
                "The path of event is mismatched. Expected:" + storePath + " " + "Actual:" + dataPath);
        }
        listener.handleStoreUpdated((Store) data);
    }

    @Override
    public void handleDataDeleted(String dataPath)
        throws Exception {
        if (!dataPath.equals(storePath)) {
            throw new IllegalArgumentException(
                "The path of event is mismatched. Expected:" + storePath + " " + "Actual:" + dataPath);
        }
        listener.handleStoreDeleted(dataPath.substring(dataPath.lastIndexOf('/') + 1));
    }
}
