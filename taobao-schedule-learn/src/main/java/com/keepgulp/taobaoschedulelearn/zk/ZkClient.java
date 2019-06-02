package com.keepgulp.taobaoschedulelearn.zk;

import com.keepgulp.taobaoschedulelearn.config.ZKConfigurationProperites;
import lombok.Data;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.ExistsBuilder;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

@Data
@Log4j2
public class ZkClient {

    @Autowired
    private ZKConfigurationProperites zkConfigurationProperites;

    private CuratorFramework client;
    private String rootPath;
    private String namespace;
    private String zookeeperServer;
    private int sessionTimeoutMs;
    private int connectionTimeoutMs;
    private int baseSleepTimeMs;
    private int maxRetries;
    private String username;
    private String password;

    public ZkClient() {
        this.setZookeeperServer(zkConfigurationProperites.getZookeeperServer());
        this.setSessionTimeoutMs(zkConfigurationProperites.getSessionTimeoutMs());
        this.setConnectionTimeoutMs(zkConfigurationProperites.getConnectionTimeoutMs());
        this.setMaxRetries(ZKConfigurationProperites.getMaxRetries());
        this.setBaseSleepTimeMs(zkConfigurationProperites.getBaseSleepTimeMs());
        this.setUsername(zkConfigurationProperites.getUsername());
        this.setPassword(zkConfigurationProperites.getPassword());
        this.setRootPath(zkConfigurationProperites.getRootPath());
        this.setNamespace(zkConfigurationProperites.getNamespace());
    }

    public void init() throws Exception {
        log.info("zookeeper初始化开始.....");
//        String authString = this.getUsername() + ":" + this.getPassword();
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(baseSleepTimeMs, maxRetries);
        client = CuratorFrameworkFactory.builder()
//                .authorization("digist", authString.getBytes())
                .connectString(zookeeperServer)
                .retryPolicy(retryPolicy)
                .sessionTimeoutMs(sessionTimeoutMs)
                .connectionTimeoutMs(connectionTimeoutMs)
                .namespace(namespace)
                .build();

        long stime = System.currentTimeMillis();
        client.start();
        log.info("zk初始化用时:{} ms ", (System.currentTimeMillis() - stime));
        // 获取当前客户端的状态
        log.info("zookeeper状态：{}", client.getState());

    }

    public void stop() {
        log.info("zookeeper停止...");
        if (client != null) {
            this.client.close();
        }
    }

    public String register(BackgroundCallback backgroundCallback, List<ACL> acl) throws Exception {
        String hostAddress = InetAddress.getLocalHost().getHostAddress();
        String serviceInstance = "server" + "-" +  hostAddress + "-";
        return client.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
                .withACL(acl)
                .inBackground(backgroundCallback)
                .forPath(this.getRootPath() + "/" + serviceInstance, hostAddress.getBytes());
    }

    public String getData(String path) {
        String result = "";
        try {
            result = new String(client.getData().forPath(path), StandardCharsets.UTF_8);
        } catch (Exception e) {
            log.error("获取子节点出错", e);
        }
        return result;
    }

    public String getData(String zkPath, Stat stat) {
        String result = "";
        try {
            result = new String(client.getData().storingStatIn(stat).forPath(zkPath), StandardCharsets.UTF_8);
        } catch (Exception e) {
            log.error("获取子节点出错", e);
        }
        return result;
    }

    public List<String> getChildren(String path) {
        List<String> childrenList = new ArrayList<>();
        try {
            childrenList = client.getChildren().forPath(path);
        } catch (Exception e) {
            log.error("获取子节点出错", e);
        }
        return childrenList;
    }

    public Stat exists(String path, boolean watch) throws Exception {
        return client.checkExists().forPath(path);
    }

    public String createNode(String path, CreateMode persistent, List<ACL> acl) throws Exception {
        return client.create()
                .creatingParentsIfNeeded().withMode(persistent)
                .withACL(acl)
                .forPath(path);
    }

    public String createNode(String path, CreateMode persistent, List<ACL> acl, String value) throws Exception {
        return client.create()
                .creatingParentsIfNeeded().withMode(persistent)
                .withACL(acl)
                .forPath(path, null == value ? null : value.getBytes());
    }

    public void deleteTree(String path) throws Exception {
        client.delete().deletingChildrenIfNeeded().forPath(path);
    }

    public Stat setData(String path, String value) throws Exception {
        return client.setData().forPath(path,  null == value ? null : value.getBytes());
    }


}
