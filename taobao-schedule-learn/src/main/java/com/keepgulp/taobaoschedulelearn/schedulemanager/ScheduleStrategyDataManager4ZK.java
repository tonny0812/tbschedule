package com.keepgulp.taobaoschedulelearn.schedulemanager;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.keepgulp.taobaoschedulelearn.serverdefine.ScheduleStrategy;
import com.keepgulp.taobaoschedulelearn.strategy.ManagerFactoryInfo;
import com.keepgulp.taobaoschedulelearn.strategy.ScheduleStrategyRunntime;
import com.keepgulp.taobaoschedulelearn.strategy.TBScheduleManagerFactory;
import com.keepgulp.taobaoschedulelearn.util.TimestampTypeAdapter;
import com.keepgulp.taobaoschedulelearn.zk.ConfigNode;
import com.keepgulp.taobaoschedulelearn.zk.ZKManager;
import com.keepgulp.taobaoschedulelearn.zk.ZkClient;
import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.CreateMode;

import java.io.Writer;
import java.sql.Timestamp;
import java.util.*;

public class ScheduleStrategyDataManager4ZK {

    private ZKManager zkManager;
    private String PATH_Strategy;
    private String PATH_ManagerFactory;
    private Gson gson;

    // 在Spring对象创建完毕后，创建内部对象
    public ScheduleStrategyDataManager4ZK(ZKManager aZkManager) throws Exception {
        this.zkManager = aZkManager;
        gson = new GsonBuilder().registerTypeAdapter(Timestamp.class, new TimestampTypeAdapter())
            .setDateFormat("yyyy-MM-dd HH:mm:ss").create();
        this.PATH_Strategy = this.getZooKeeper().getRootPath() + "/strategy";
        this.PATH_ManagerFactory = this.getZooKeeper().getRootPath() + "/factory";

        if (this.getZooKeeper().exists(this.PATH_Strategy, false) == null) {
            this.getZooKeeper().createNode(this.PATH_Strategy, CreateMode.PERSISTENT, this.zkManager.getAcl());
        }
        if (this.getZooKeeper().exists(this.PATH_ManagerFactory, false) == null) {
            this.getZooKeeper()
                .createNode(this.PATH_ManagerFactory, CreateMode.PERSISTENT, this.zkManager.getAcl());
        }
    }

    public ScheduleStrategy loadStrategy(String strategyName) throws Exception {
        String zkPath = this.PATH_Strategy + "/" + strategyName;
        if (this.getZooKeeper().exists(zkPath, false) == null) {
            return null;
        }
        String valueString = new String(this.getZooKeeper().getData(zkPath));
        ScheduleStrategy result = (ScheduleStrategy) this.gson.fromJson(valueString, ScheduleStrategy.class);
        return result;
    }

    public void createScheduleStrategy(ScheduleStrategy scheduleStrategy) throws Exception {
        String zkPath = this.PATH_Strategy + "/" + scheduleStrategy.getStrategyName();
        String valueString = this.gson.toJson(scheduleStrategy);
        if (this.getZooKeeper().exists(zkPath, false) == null) {
            this.getZooKeeper().createNode(zkPath, CreateMode.PERSISTENT, this.zkManager.getAcl(), valueString);
        } else {
            throw new Exception("调度策略" + scheduleStrategy.getStrategyName()
                + "已经存在,如果确认需要重建，请先调用deleteMachineStrategy(String taskType)删除");
        }
    }

    public void updateScheduleStrategy(ScheduleStrategy scheduleStrategy) throws Exception {
        String zkPath = this.PATH_Strategy + "/" + scheduleStrategy.getStrategyName();
        String valueString = this.gson.toJson(scheduleStrategy);
        if (this.getZooKeeper().exists(zkPath, false) == null) {
            this.getZooKeeper().createNode(zkPath, CreateMode.PERSISTENT, this.zkManager.getAcl(), valueString);
        } else {
            this.getZooKeeper().setData(zkPath, valueString);
        }

    }

    public void deleteMachineStrategy(String taskType) throws Exception {
        deleteMachineStrategy(taskType, false);
    }

    public void pause(String strategyName) throws Exception {
        ScheduleStrategy strategy = this.loadStrategy(strategyName);
        strategy.setSts(ScheduleStrategy.STS_PAUSE);
        this.updateScheduleStrategy(strategy);
    }

    public void resume(String strategyName) throws Exception {
        ScheduleStrategy strategy = this.loadStrategy(strategyName);
        strategy.setSts(ScheduleStrategy.STS_RESUME);
        this.updateScheduleStrategy(strategy);
    }

    public void deleteMachineStrategy(String taskType, boolean isForce) throws Exception {
        String zkPath = this.PATH_Strategy + "/" + taskType;
        if (isForce == false && this.getZooKeeper().getChildren(zkPath).size() > 0) {
            throw new Exception("不能删除" + taskType + "的运行策略，会导致必须重启整个应用才能停止失去控制的调度进程。" + "可以先清空IP地址，等所有的调度器都停止后再删除调度策略");
        }
        this.getZooKeeper().deleteTree( zkPath);
    }

    public List<ScheduleStrategy> loadAllScheduleStrategy() throws Exception {
        String zkPath = this.PATH_Strategy;
        List<ScheduleStrategy> result = new ArrayList<ScheduleStrategy>();
        List<String> names = this.getZooKeeper().getChildren(zkPath);
        Collections.sort(names);
        for (String name : names) {
            result.add(this.loadStrategy(name));
        }
        return result;
    }

    /**
     * 注册ManagerFactory
     *
     * @return 需要全部注销的调度，例如当IP不在列表中
     */
    public List<String> registerManagerFactory(TBScheduleManagerFactory managerFactory) throws Exception {

        if (managerFactory.getUuid() == null) {
            String uuid =
                managerFactory.getIp() + "$" + managerFactory.getHostName() + "$" + UUID.randomUUID().toString()
                    .replaceAll("-", "").toUpperCase();
            String zkPath = this.PATH_ManagerFactory + "/" + uuid + "$";
            zkPath = this.getZooKeeper().createNode(zkPath, CreateMode.EPHEMERAL_SEQUENTIAL, this.zkManager.getAcl());
            managerFactory.setUuid(zkPath.substring(zkPath.lastIndexOf("/") + 1));
        } else {
            String zkPath = this.PATH_ManagerFactory + "/" + managerFactory.getUuid();
            if (this.getZooKeeper().exists(zkPath, false) == null) {
                zkPath = this.getZooKeeper().createNode(zkPath, CreateMode.EPHEMERAL, this.zkManager.getAcl());
            }
        }

        List<String> result = new ArrayList<String>();
        for (ScheduleStrategy scheduleStrategy : loadAllScheduleStrategy()) {
            boolean isFind = false;
            // 暂停或者不在IP范围
            if (ScheduleStrategy.STS_PAUSE.equalsIgnoreCase(scheduleStrategy.getSts()) == false
                && scheduleStrategy.getIPList() != null) {
                for (String ip : scheduleStrategy.getIPList()) {
                    if (ip.equals("127.0.0.1") || ip.equalsIgnoreCase("localhost") || ip.equals(managerFactory.getIp())
                        || ip.equalsIgnoreCase(managerFactory.getHostName())) {
                        // 添加可管理TaskType
                        String zkPath =
                            this.PATH_Strategy + "/" + scheduleStrategy.getStrategyName() + "/" + managerFactory
                                .getUuid();
                        if (this.getZooKeeper().exists(zkPath, false) == null) {
                            zkPath = this.getZooKeeper()
                                .createNode(zkPath, CreateMode.EPHEMERAL, this.zkManager.getAcl());
                        }
                        isFind = true;
                        break;
                    }
                }
            }
            if (isFind == false) {// 清除原来注册的Factory
                String zkPath =
                    this.PATH_Strategy + "/" + scheduleStrategy.getStrategyName() + "/" + managerFactory.getUuid();
                if (this.getZooKeeper().exists(zkPath, false) != null) {
                    this.getZooKeeper().deleteTree(zkPath);
                    result.add(scheduleStrategy.getStrategyName());
                }
            }
        }
        return result;
    }

    /**
     * 注销服务，停止调度
     */
    public void unRregisterManagerFactory(TBScheduleManagerFactory managerFactory) throws Exception {
        for (String taskName : this.getZooKeeper().getChildren(this.PATH_Strategy)) {
            String zkPath = this.PATH_Strategy + "/" + taskName + "/" + managerFactory.getUuid();
            if (this.getZooKeeper().exists(zkPath, false) != null) {
                this.getZooKeeper().deleteTree(zkPath);
            }
        }
    }

    public ScheduleStrategyRunntime loadScheduleStrategyRunntime(String strategyName, String uuid) throws Exception {
        String zkPath = this.PATH_Strategy + "/" + strategyName + "/" + uuid;
        ScheduleStrategyRunntime result = null;
        if (this.getZooKeeper().exists(zkPath, false) != null) {
            String value = this.getZooKeeper().getData(zkPath);
            if (!StringUtils.isEmpty(value)) {
                result = this.gson.fromJson(value, ScheduleStrategyRunntime.class);
                if (null == result) {
                    throw new Exception("gson 反序列化异常,对象为null");
                }
                if (null == result.getStrategyName()) {
                    throw new Exception("gson 反序列化异常,策略名字为null");
                }
                if (null == result.getUuid()) {
                    throw new Exception("gson 反序列化异常,uuid为null");
                }
            } else {
                result = new ScheduleStrategyRunntime();
                result.setStrategyName(strategyName);
                result.setUuid(uuid);
                result.setRequestNum(0);
                result.setMessage("");
            }
        }
        return result;
    }

    /**
     * 装载所有的策略运行状态
     */
    public List<ScheduleStrategyRunntime> loadAllScheduleStrategyRunntime() throws Exception {
        List<ScheduleStrategyRunntime> result = new ArrayList<ScheduleStrategyRunntime>();
        String zkPath = this.PATH_Strategy;
        for (String taskType : this.getZooKeeper().getChildren(zkPath)) {
            for (String uuid : this.getZooKeeper().getChildren(zkPath + "/" + taskType)) {
                result.add(loadScheduleStrategyRunntime(taskType, uuid));
            }
        }
        return result;
    }

    public List<ScheduleStrategyRunntime> loadAllScheduleStrategyRunntimeByUUID(String managerFactoryUUID)
        throws Exception {
        List<ScheduleStrategyRunntime> result = new ArrayList<ScheduleStrategyRunntime>();
        String zkPath = this.PATH_Strategy;

        List<String> taskTypeList = this.getZooKeeper().getChildren(zkPath);
        Collections.sort(taskTypeList);
        for (String taskType : taskTypeList) {
            if (this.getZooKeeper().exists(zkPath + "/" + taskType + "/" + managerFactoryUUID, false) != null) {
                result.add(loadScheduleStrategyRunntime(taskType, managerFactoryUUID));
            }
        }
        return result;
    }

    public List<ScheduleStrategyRunntime> loadAllScheduleStrategyRunntimeByTaskType(String strategyName)
        throws Exception {
        List<ScheduleStrategyRunntime> result = new ArrayList<ScheduleStrategyRunntime>();
        String zkPath = this.PATH_Strategy;
        if (this.getZooKeeper().exists(zkPath + "/" + strategyName, false) == null) {
            return result;
        }
        List<String> uuidList = this.getZooKeeper().getChildren(zkPath + "/" + strategyName);
        // 排序
        Collections.sort(uuidList, new Comparator<String>() {
            public int compare(String u1, String u2) {
                return u1.substring(u1.lastIndexOf("$") + 1).compareTo(u2.substring(u2.lastIndexOf("$") + 1));
            }
        });

        for (String uuid : uuidList) {
            result.add(loadScheduleStrategyRunntime(strategyName, uuid));
        }
        return result;
    }

    /**
     * 更新请求数量
     */
    public void updateStrategyRunntimeReqestNum(String strategyName, String manangerFactoryUUID, int requestNum)
        throws Exception {
        String zkPath = this.PATH_Strategy + "/" + strategyName + "/" + manangerFactoryUUID;
        ScheduleStrategyRunntime result = null;
        if (this.getZooKeeper().exists(zkPath, false) != null) {
            result = this.loadScheduleStrategyRunntime(strategyName, manangerFactoryUUID);
        } else {
            result = new ScheduleStrategyRunntime();
            result.setStrategyName(strategyName);
            result.setUuid(manangerFactoryUUID);
            result.setRequestNum(requestNum);
            result.setMessage("");
        }
        result.setRequestNum(requestNum);
        String valueString = this.gson.toJson(result);
        this.getZooKeeper().setData(zkPath, valueString);
    }

    /**
     * 更新调度过程中的信息
     */
    public void updateStrategyRunntimeErrorMessage(String strategyName, String manangerFactoryUUID, String message)
        throws Exception {
        String zkPath = this.PATH_Strategy + "/" + strategyName + "/" + manangerFactoryUUID;
        ScheduleStrategyRunntime result = null;
        if (this.getZooKeeper().exists(zkPath, false) != null) {
            result = this.loadScheduleStrategyRunntime(strategyName, manangerFactoryUUID);
        } else {
            result = new ScheduleStrategyRunntime();
            result.setStrategyName(strategyName);
            result.setUuid(manangerFactoryUUID);
            result.setRequestNum(0);
        }
        result.setMessage(message);
        String valueString = this.gson.toJson(result);
        this.getZooKeeper().setData(zkPath, valueString);
    }

    public void updateManagerFactoryInfo(String uuid, boolean isStart) throws Exception {
        String zkPath = this.PATH_ManagerFactory + "/" + uuid;
        if (this.getZooKeeper().exists(zkPath, false) == null) {
            throw new Exception("任务管理器不存在:" + uuid);
        }
        this.getZooKeeper().setData(zkPath, Boolean.toString(isStart));
    }

    public ManagerFactoryInfo loadManagerFactoryInfo(String uuid) throws Exception {
        String zkPath = this.PATH_ManagerFactory + "/" + uuid;
        if (this.getZooKeeper().exists(zkPath, false) == null) {
            throw new Exception("任务管理器不存在:" + uuid);
        }
        String value = this.getZooKeeper().getData(zkPath);
        ManagerFactoryInfo result = new ManagerFactoryInfo();
        result.setUuid(uuid);
        if (value == null) {
            result.setStart(true);
        } else {
            result.setStart(Boolean.parseBoolean(value));
        }
        return result;
    }

    /**
     * 导入配置信息【目前支持baseTaskType和strategy数据】
     */
    public void importConfig(String config, Writer writer, boolean isUpdate) throws Exception {
        ConfigNode configNode = gson.fromJson(config, ConfigNode.class);
        if (configNode != null) {
            String path = configNode.getRootPath() + "/" + configNode.getConfigType();
            this.getZooKeeper().createNode(path, CreateMode.PERSISTENT, zkManager.getAcl());
            String y_node = path + "/" + configNode.getName();
            if (getZooKeeper().exists(y_node, false) == null) {
                writer.append("<font color=\"red\">成功导入新配置信息\n</font>");
                this.getZooKeeper()
                    .createNode(y_node, CreateMode.PERSISTENT, zkManager.getAcl(), configNode.getValue());
            } else if (isUpdate) {
                writer.append("<font color=\"red\">该配置信息已经存在，并且强制更新了\n</font>");
                this.getZooKeeper().setData(y_node, configNode.getValue());
            } else {
                writer.append("<font color=\"red\">该配置信息已经存在，如果需要更新，请配置强制更新\n</font>");
            }
        }
        writer.append(configNode.toString());
    }

    /**
     * 输出配置信息【目前备份baseTaskType和strategy数据】
     */
    public StringBuffer exportConfig(String rootPath, Writer writer) throws Exception {
        StringBuffer buffer = new StringBuffer();
        for (String type : new String[]{"baseTaskType", "strategy"}) {
            if (type.equals("baseTaskType")) {
                writer.write("<h2>基本任务配置列表：</h2>\n");
            } else {
                writer.write("<h2>基本策略配置列表：</h2>\n");
            }
            String bTTypePath = rootPath + "/" + type;
            List<String> fNodeList = getZooKeeper().getChildren(bTTypePath);
            for (int i = 0; i < fNodeList.size(); i++) {
                String fNode = fNodeList.get(i);
                ConfigNode configNode = new ConfigNode(rootPath, type, fNode);
                configNode.setValue(this.getZooKeeper().getData(bTTypePath + "/" + fNode));
                buffer.append(gson.toJson(configNode));
                buffer.append("\n");
                writer.write(configNode.toString());
            }
            writer.write("\n\n");
        }
        if (buffer.length() > 0) {
            String str = buffer.toString();
            return new StringBuffer(str.substring(0, str.length() - 1));
        }
        return buffer;
    }

    public List<ManagerFactoryInfo> loadAllManagerFactoryInfo() throws Exception {
        String zkPath = this.PATH_ManagerFactory;
        List<ManagerFactoryInfo> result = new ArrayList<ManagerFactoryInfo>();
        List<String> names = this.getZooKeeper().getChildren(zkPath);
        Collections.sort(names, new Comparator<String>() {
            public int compare(String u1, String u2) {
                return u1.substring(u1.lastIndexOf("$") + 1).compareTo(u2.substring(u2.lastIndexOf("$") + 1));
            }
        });
        for (String name : names) {
            ManagerFactoryInfo info = new ManagerFactoryInfo();
            info.setUuid(name);
            String value = this.getZooKeeper().getData(zkPath + "/" + name);
            if (value == null) {
                info.setStart(true);
            } else {
                info.setStart(Boolean.parseBoolean(value));
            }
            result.add(info);
        }
        return result;
    }

    public void printTree(String path, Writer writer, String lineSplitChar) throws Exception {
        this.getZooKeeper().printTree(path, writer, lineSplitChar);
    }

    public void deleteTree(String path) throws Exception {
        this.getZooKeeper().deleteTree( path);
    }

    public ZkClient getZooKeeper() throws Exception {
        return this.zkManager.getZkClient();
    }

    public String getRootPath() {
        return this.zkManager.getZkClient().getRootPath();
    }
}
