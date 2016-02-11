package com.linkedin.venice.helix;

import com.linkedin.venice.meta.MetadataRepository;
import com.linkedin.venice.meta.Store;
import com.sun.istack.internal.NotNull;
import org.apache.helix.AccessOption;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.log4j.Logger;


/**
 * Use helix as storage for metadata. Provide the basic operations to communicate with Helix to access to metadata.
 */
public class HelixMetadataRepository implements MetadataRepository {
    private static final Logger logger = Logger.getLogger(HelixMetadataRepository.class.getName());

    /**
     * Data accessor of Zookeeper
     */
    protected ZkBaseDataAccessor<Store> dataAccessor;
    /**
     * Root path of stores in Zookeeper.
     */
    protected final String rootPath;

    public HelixMetadataRepository(@NotNull String zkAddress, @NotNull String rootPath) {
        this.rootPath = rootPath;
        dataAccessor = new ZkBaseDataAccessor<>(
            new ZkClient(zkAddress, ZkClient.DEFAULT_SESSION_TIMEOUT, ZkClient.DEFAULT_CONNECTION_TIMEOUT,
                new BasicStoreSerializer()));
    }

    public HelixMetadataRepository(@NotNull ZkClient zkClient, @NotNull String rootPath) {
        this.rootPath = rootPath;
        dataAccessor = new ZkBaseDataAccessor<>(zkClient);
    }

    @Override
    public Store getStore(@NotNull String name) {
        return dataAccessor.get(composeStorePath(name), null, AccessOption.PERSISTENT);
    }

    @Override
    public void deleteStore(@NotNull String name) {
        dataAccessor.remove(composeStorePath(name), AccessOption.PERSISTENT);
    }

    @Override
    public void addStore(@NotNull Store store) {
        if (dataAccessor.exists(composeStorePath(store.getName()), AccessOption.PERSISTENT)) {
            throw new IllegalArgumentException("Store" + store.getName() + " is existed.");
        }
        dataAccessor.set(composeStorePath(store.getName()), store, AccessOption.PERSISTENT);
    }

    @Override
    public void updateStore(@NotNull Store store) {
        if (!dataAccessor.exists(composeStorePath(store.getName()), AccessOption.PERSISTENT)) {
            throw new IllegalArgumentException("Store" + store.getName() + " is not existed.");
        }
        dataAccessor.set(composeStorePath(store.getName()), store, AccessOption.PERSISTENT);
    }

    protected String composeStorePath(String name) {
        return this.rootPath + "/" + name;
    }

    protected String extractStoreNameFromPath(String path) {
        if (!path.startsWith(rootPath)) {
            throw new IllegalArgumentException("Path:" + path + " is invalid.");
        }
        return path.substring(rootPath.length() + 1);
    }

    public String getRootPath() {
        return rootPath;
    }
}
