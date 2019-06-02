package com.keepgulp.taobaoschedulelearn.zk;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ZKManager {

    @Autowired
    private ZkClient zkClient;


}
