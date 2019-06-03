package com.keepgulp.taobaoschedulelearn.zk;

import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Log4j2
@Component
public class ZKManager {

    @Getter
    private List<ACL> acl = new ArrayList<>();

    @Getter
    private ZkClient zkClient;

    public ZKManager(){
        try {
            log.info("================ZKManager()初始化=================");
            this.connect();

            acl.clear();
            String authString = zkClient.getUsername() + ":" + zkClient.getPassword();
            acl.add(new ACL(ZooDefs.Perms.ALL, new Id("digest", DigestAuthenticationProvider.generateDigest(authString))));
            acl.add(new ACL(ZooDefs.Perms.READ, ZooDefs.Ids.ANYONE_ID_UNSAFE));

            this.zkClient.register(new BackgroundCallback() {
                @Override
                public void processResult(CuratorFramework client, CuratorEvent curatorEvent) throws Exception {
                    log.info(curatorEvent.getName(), curatorEvent.getType());
                }
            }, acl);
        } catch (Exception e) {
            log.error("zk连接出错", e);
        }
    }
    /**
     * zk连接
     * @throws Exception
     */
    public synchronized void connect() throws Exception {
        if (this.zkClient != null) {
            this.zkClient.stop();
            this.zkClient = null;
        }

        zkClient = new ZkClient();

        zkClient.init();
    }

    public void close() throws InterruptedException {
        log.info("关闭zookeeper连接");
        if (zkClient == null) {
            return;
        }
        this.zkClient.stop();
    }

    public boolean checkZookeeperState() {
        return zkClient != null && zkClient.getState() == CuratorFrameworkState.STARTED;
    }
}
