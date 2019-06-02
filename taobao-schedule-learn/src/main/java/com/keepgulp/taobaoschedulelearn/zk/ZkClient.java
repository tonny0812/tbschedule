package com.keepgulp.taobaoschedulelearn.zk;

import lombok.Data;
import lombok.extern.log4j.Log4j2;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
@Log4j2
public class ZkClient {

    private final static String rootPath = "/" + "services";

    private CuratorFramework client;
    private String zookeeperServer;
    private int sessionTimeoutMs;
    private int connectionTimeoutMs;
    private int baseSleepTimeMs;
    private int maxRetries;

    public void init() {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(baseSleepTimeMs, maxRetries);
        client = CuratorFrameworkFactory.builder().connectString(zookeeperServer).retryPolicy(retryPolicy)
                .sessionTimeoutMs(sessionTimeoutMs).connectionTimeoutMs(connectionTimeoutMs).build();
        client.start();
    }

    public void stop() {
        client.close();
    }

    public void register() {
        try {
            String hostAddress = InetAddress.getLocalHost().getHostAddress();
            String serviceInstance = "prometheus" + "-" +  hostAddress + "-";
            client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(rootPath + "/" + serviceInstance);
        } catch (Exception e) {
            log.error("注册出错", e);
        }
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

    public List<String> getChildren(String path) {
        List<String> childrenList = new ArrayList<>();
        try {
            childrenList = client.getChildren().forPath(path);
        } catch (Exception e) {
            log.error("获取子节点出错", e);
        }
        return childrenList;
    }

    public int getChildrenCount(String path) {
        return getChildren(path).size();
    }

    public List<String> getInstances() {
        return getChildren(rootPath);
    }

    public int getInstancesCount() {
        return getInstances().size();
    }

    public Map<String, String> getChildrenData() {
        Map<String, String> resultMap = new HashMap<>();
        List<String> childrenNames = getInstances();
        for(String name : childrenNames) {
            String cpath = rootPath + "/" + name;
            resultMap.put(cpath, getData(cpath));
        }
        return resultMap;
    }
}
