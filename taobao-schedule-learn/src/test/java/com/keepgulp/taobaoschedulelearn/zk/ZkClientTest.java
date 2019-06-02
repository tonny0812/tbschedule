package com.keepgulp.taobaoschedulelearn.zk;

import lombok.extern.log4j.Log4j2;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;
import java.util.Map;

@RunWith(SpringRunner.class)
@SpringBootTest
@Log4j2
public class ZkClientTest {

    @Autowired
    private ZkClient zkClient;

    @Test
    public void test() {
        Map<String, String> data = zkClient.getChildrenData();
        for (String cpath : data.keySet()) {
            log.info("路径：{} 值：{}", cpath, data.get(cpath));
        }
     }

}