package com.linkedin.venice.helix;

import com.linkedin.venice.meta.StoreListChangedListener;
import java.util.List;
import javax.validation.constraints.NotNull;
import org.I0Itec.zkclient.IZkChildListener;


/**
 * Listener handle the children changed event. In order to avoid exposing I0ITec API, this listener used as a bridge to
 * convert zookeeper event to Venice event.
 */
class HelixStoreListChangedListener implements IZkChildListener {
    private final String rootPath;
    private final StoreListChangedListener listener;

    public HelixStoreListChangedListener(@NotNull String rootPath, @NotNull StoreListChangedListener listener) {
        this.rootPath = rootPath;
        this.listener = listener;
    }

    @Override
    public void handleChildChange(String parentPath, List<String> currentChilds)
        throws Exception {
        if (!parentPath.equals(rootPath)) {
            throw new IllegalArgumentException(
                "The path of event is mismatched. Expected:" + rootPath + " Actual:" + parentPath);
        }
        listener.handleStoreListChanged(currentChilds);
    }
}
