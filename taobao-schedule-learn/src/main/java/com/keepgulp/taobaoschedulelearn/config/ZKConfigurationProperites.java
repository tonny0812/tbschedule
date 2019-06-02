package com.keepgulp.taobaoschedulelearn.config;

import lombok.Getter;
import lombok.ToString;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@ToString
@Component
public class ZKConfigurationProperites {

    @Getter
    private static String zookeeperServer;

    @Getter
    private static int sessionTimeoutMs;

    @Getter
    private static int connectionTimeoutMs;

    @Getter
    private static int maxRetries;

    @Getter
    private static int baseSleepTimeMs;

    @Getter
    private static String username;

    @Getter
    private static String password;

    @Getter
    private static String rootPath;

    @Getter
    private static String namespace;

    @Value("${zookeeper.server}")
    public void setZookeeperServer(String zookeeperServer) {
        ZKConfigurationProperites.zookeeperServer = zookeeperServer;
    }

    @Value("${zookeeper.sessionTimeoutMs}")
    public void setSessionTimeoutMs(int sessionTimeoutMs) {
        ZKConfigurationProperites.sessionTimeoutMs = sessionTimeoutMs;
    }

    @Value("${zookeeper.connectionTimeoutMs}")
    public void setConnectionTimeoutMs(int connectionTimeoutMs) {
        ZKConfigurationProperites.connectionTimeoutMs = connectionTimeoutMs;
    }

    @Value("${zookeeper.maxRetries}")
    public void setMaxRetries(int maxRetries) {
        ZKConfigurationProperites.maxRetries = maxRetries;
    }

    @Value("${zookeeper.baseSleepTimeMs}")
    public void setBaseSleepTimeMs(int baseSleepTimeMs) {
        ZKConfigurationProperites.baseSleepTimeMs = baseSleepTimeMs;
    }

    @Value("${zookeeper.username}")
    public void setUsername(String username) {
        ZKConfigurationProperites.username = username;
    }

    @Value("${zookeeper.password}")
    public void setPassword(String password) {
        ZKConfigurationProperites.password = password;
    }

    @Value("${zk.server.root.path}")
    public void setRootPath(String rootPath) {
        ZKConfigurationProperites.rootPath = rootPath;
    }

    @Value("${zk.server.namespace}")
    public void setNamespace(String namespace) {
        ZKConfigurationProperites.namespace = namespace;
    }
}
