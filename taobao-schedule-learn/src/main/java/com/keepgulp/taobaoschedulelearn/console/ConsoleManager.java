package com.keepgulp.taobaoschedulelearn.console;

import com.keepgulp.taobaoschedulelearn.schedulemanager.IScheduleDataManager;
import com.keepgulp.taobaoschedulelearn.schedulemanager.ScheduleStrategyDataManager4ZK;
import com.keepgulp.taobaoschedulelearn.strategy.TBScheduleManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Properties;

public class ConsoleManager {

    protected static transient Logger log = LoggerFactory.getLogger(ConsoleManager.class);

    public final static String configFile =
//        System.getProperty("user.dir") + File.separator + "pamirsScheduleConfig.properties";
        "/data/tbschedule" + File.separator + "pamirsScheduleConfig.properties";

    private static TBScheduleManagerFactory scheduleManagerFactory;

    public static boolean isInitial() throws Exception {
        return scheduleManagerFactory != null;
    }

    public static boolean initial() throws Exception {
        if (scheduleManagerFactory != null) {
            return true;
        }
        File file = new File(configFile);
        scheduleManagerFactory = new TBScheduleManagerFactory();
        scheduleManagerFactory.start = false;

        if (file.exists() == true) {
            // Console不启动调度能力
            Properties p = new Properties();
            FileReader reader = new FileReader(file);
            p.load(reader);
            reader.close();
            scheduleManagerFactory.init(p);
            log.info("加载Schedule配置文件：" + configFile);
            return true;
        } else {
            return false;
        }
    }

    public static TBScheduleManagerFactory getScheduleManagerFactory() throws Exception {
        if (isInitial() == false) {
            initial();
        }
        return scheduleManagerFactory;
    }

    public static IScheduleDataManager getScheduleDataManager() throws Exception {
        if (isInitial() == false) {
            initial();
        }
        return scheduleManagerFactory.getScheduleDataManager();
    }

    public static ScheduleStrategyDataManager4ZK getScheduleStrategyManager() throws Exception {
        if (isInitial() == false) {
            initial();
        }
        return scheduleManagerFactory.getScheduleStrategyManager();
    }

    public static void saveConfigInfo(Properties p) throws Exception {
        try {
            FileWriter writer = new FileWriter(configFile);
            p.store(writer, "");
            writer.close();
        } catch (Exception ex) {
            throw new Exception("不能写入配置信息到文件：" + configFile, ex);
        }
        if (scheduleManagerFactory == null) {
            initial();
        } else {
            scheduleManagerFactory.reInit(p);
        }
    }

    public static void setScheduleManagerFactory(TBScheduleManagerFactory scheduleManagerFactory) {
        ConsoleManager.scheduleManagerFactory = scheduleManagerFactory;
    }

}
