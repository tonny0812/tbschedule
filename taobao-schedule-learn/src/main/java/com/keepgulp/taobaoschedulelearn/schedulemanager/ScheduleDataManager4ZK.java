package com.keepgulp.taobaoschedulelearn.schedulemanager;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.keepgulp.taobaoschedulelearn.serverdefine.ScheduleServer;
import com.keepgulp.taobaoschedulelearn.serverdefine.ScheduleServerComparator;
import com.keepgulp.taobaoschedulelearn.taskdefine.ScheduleTaskItem;
import com.keepgulp.taobaoschedulelearn.taskdefine.ScheduleTaskType;
import com.keepgulp.taobaoschedulelearn.taskdefine.ScheduleTaskTypeRunningInfo;
import com.keepgulp.taobaoschedulelearn.taskdefine.TaskItemDefine;
import com.keepgulp.taobaoschedulelearn.util.ScheduleUtil;
import com.keepgulp.taobaoschedulelearn.util.TimestampTypeAdapter;
import com.keepgulp.taobaoschedulelearn.zk.ZKManager;
import com.keepgulp.taobaoschedulelearn.zk.ZkClient;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import java.sql.Timestamp;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Log4j2
public class ScheduleDataManager4ZK implements IScheduleDataManager {

    private Gson gson;
    private ZKManager zkManager;
    private String PATH_BaseTaskType;
    private String PATH_TaskItem = "taskItem";
    private String PATH_Server = "server";
    private long zkBaseTime = 0;
    private long loclaBaseTime = 0;

    public ScheduleDataManager4ZK(ZKManager aZkManager) throws Exception {
        this.zkManager = aZkManager;
        gson = new GsonBuilder().registerTypeAdapter(Timestamp.class, new TimestampTypeAdapter())
                .setDateFormat("yyyy-MM-dd HH:mm:ss").create();


        ZkClient zkClient = this.getZooKeeper();
        this.PATH_BaseTaskType = zkClient.getRootPath() + "/baseTaskType";

        if (zkClient.exists(this.PATH_BaseTaskType, false) == null) {
            zkClient.createNode(this.PATH_BaseTaskType, CreateMode.PERSISTENT, this.zkManager.getAcl());
        }
        loclaBaseTime = System.currentTimeMillis();
        String tempPath = zkClient.createNode(zkClient.getRootPath() + "/systime",
                CreateMode.EPHEMERAL_SEQUENTIAL, this.zkManager.getAcl());
        Stat tempStat = zkClient.exists(tempPath, false);
        zkBaseTime = tempStat.getCtime();
        zkClient.deleteTree(tempPath);
        if (Math.abs(this.zkBaseTime - this.loclaBaseTime) > 5000) {
            log.error("请注意，Zookeeper服务器时间与本地时间相差 ： " + Math.abs(this.zkBaseTime - this.loclaBaseTime) + " ms");
        }
    }

    private ZkClient getZooKeeper() {
        return this.zkManager.getZkClient();
    }

    @Override
    public long getSystemTime() {
        return this.zkBaseTime + (System.currentTimeMillis() - this.loclaBaseTime);
    }

    @Override
    public List<TaskItemDefine> reloadDealTaskItem(String taskType, String uuid) throws Exception {
        String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(taskType);
        String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType + "/" + taskType + "/" + this.PATH_TaskItem;

        List<String> taskItems = this.getZooKeeper().getChildren(zkPath);
        // Collections.sort(taskItems);
        // 有些任务分片，业务方其实是用数字的字符串排序的。优先以字符串方式进行排序
        taskItems = listSort(taskItems);

        log.debug(taskType + " current uid=" + uuid + " , zk  reloadDealTaskItem");

        List<TaskItemDefine> result = new ArrayList<>();
        for (String name : taskItems) {
            String value = this.getZooKeeper().getData(zkPath + "/" + name + "/cur_server");
            if (!StringUtils.isEmpty(value) && uuid.equals(value)) {
                TaskItemDefine item = new TaskItemDefine();
                item.setTaskItemId(name);
                String parameterValue = this.getZooKeeper().getData(zkPath + "/" + name + "/parameter");
                if (!StringUtils.isEmpty(value)) {
                    item.setParameter(parameterValue);
                }
                result.add(item);

            } else if (value != null && !uuid.equals(value)) {
                log.trace(" current uid=" + uuid + " , zk cur_server uid=" + value);
            } else {
                log.trace(" current uid=" + uuid);
            }
        }
        return result;
    }

    @Override
    public List<ScheduleTaskItem> loadAllTaskItem(String taskType) throws Exception {
        List<ScheduleTaskItem> result = new ArrayList<>();
        String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(taskType);
        String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType + "/" + taskType + "/" + this.PATH_TaskItem;
        if (this.getZooKeeper().exists(zkPath, false) == null) {
            return result;
        }
        List<String> taskItems = this.getZooKeeper().getChildren(zkPath);
        // Collections.sort(taskItems);
        // 20150323 有些任务分片，业务方其实是用数字的字符串排序的。优先以数字进行排序，否则以字符串排序
        taskItems = listSort(taskItems);
        for (String taskItem : taskItems) {
            ScheduleTaskItem info = new ScheduleTaskItem();
            info.setTaskType(taskType);
            info.setTaskItem(taskItem);
            String zkTaskItemPath = zkPath + "/" + taskItem;
            String curContent = this.getZooKeeper().getData(zkTaskItemPath + "/cur_server");
            if (!StringUtils.isEmpty(curContent)) {
                info.setCurrentScheduleServer(curContent);
            }
            String reqContent = this.getZooKeeper().getData(zkTaskItemPath + "/req_server");
            if (!StringUtils.isEmpty(reqContent) ) {
                info.setRequestScheduleServer(reqContent);
            }
            String stsContent = this.getZooKeeper().getData(zkTaskItemPath + "/sts");
            if (!StringUtils.isEmpty(stsContent)) {
                info.setSts(ScheduleTaskItem.TaskItemSts.valueOf(stsContent));
            }
            String parameterContent = this.getZooKeeper().getData(zkTaskItemPath + "/parameter");
            if (!StringUtils.isEmpty(parameterContent)) {
                info.setDealParameter(parameterContent);
            }
            String dealDescContent = this.getZooKeeper().getData(zkTaskItemPath + "/deal_desc");
            if (!StringUtils.isEmpty(dealDescContent)) {
                info.setDealDesc(dealDescContent);
            }
            result.add(info);
        }
        return result;
    }

    @Override
    public void releaseDealTaskItem(String taskType, String uuid) throws Exception {
        String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(taskType);
        String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType + "/" + taskType + "/" + this.PATH_TaskItem;
        boolean isModify = false;
        for (String name : this.getZooKeeper().getChildren(zkPath)) {
            String curServerValue = this.getZooKeeper().getData(zkPath + "/" + name + "/cur_server");
            String reqServerValue = this.getZooKeeper().getData(zkPath + "/" + name + "/req_server");
            if (reqServerValue != null && curServerValue != null && uuid.equals(new String(curServerValue)) == true) {
                this.getZooKeeper().setData(zkPath + "/" + name + "/cur_server", reqServerValue);
                this.getZooKeeper().setData(zkPath + "/" + name + "/req_server", null);
                isModify = true;
            }
        }
        // 设置需要所有的服务器重新装载任务
        if (isModify == true) {
            this.updateReloadTaskItemFlag(taskType);
        }
    }

    @Override
    public int queryTaskItemCount(String taskType) {
        String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(taskType);
        String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType + "/" + taskType + "/" + this.PATH_TaskItem;
        return this.getZooKeeper().getChildren(zkPath).size();
    }

    @Override
    public ScheduleTaskType loadTaskTypeBaseInfo(String baseTaskType) throws Exception {
        String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType;
        if (this.getZooKeeper().exists(zkPath, false) == null) {
            return null;
        }
        String valueString = this.getZooKeeper().getData(zkPath);
        ScheduleTaskType result = this.gson.fromJson(valueString, ScheduleTaskType.class);
        return result;
    }

    @Override
    public int clearExpireScheduleServer(String taskType, long expireTime) throws Exception {
        int result = 0;
        String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(taskType);
        String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType + "/" + taskType + "/" + this.PATH_Server;
        if (this.getZooKeeper().exists(zkPath, false) == null) {
            String tempPath = this.PATH_BaseTaskType + "/" + baseTaskType + "/" + taskType;
            if (this.getZooKeeper().exists(tempPath, false) == null) {
                this.getZooKeeper().createNode(tempPath, CreateMode.PERSISTENT, this.zkManager.getAcl());
            }
            this.getZooKeeper().createNode(zkPath, CreateMode.PERSISTENT, this.zkManager.getAcl());
        }
        for (String name : this.getZooKeeper().getChildren(zkPath)) {
            try {
                Stat stat = this.getZooKeeper().exists(zkPath + "/" + name, false);
                if (getSystemTime() - stat.getMtime() > expireTime) {
                    this.getZooKeeper().deleteTree(zkPath + "/" + name);
                    result++;
                }
            } catch (Exception e) {
                // 当有多台服务器时，存在并发清理的可能，忽略异常
                result++;
            }
        }
        return result;
    }

    @Override
    public int clearTaskItem(String taskType, List<String> serverList) throws Exception {
        String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(taskType);
        String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType + "/" + taskType + "/" + this.PATH_TaskItem;

        int result = 0;
        for (String name : this.getZooKeeper().getChildren(zkPath)) {
            String curServerValue = this.getZooKeeper().getData(zkPath + "/" + name + "/cur_server");
            if (curServerValue != null) {
                String curServer = curServerValue;
                boolean isFind = false;
                for (String server : serverList) {
                    if (curServer.equals(server)) {
                        isFind = true;
                        break;
                    }
                }
                if (isFind == false) {
                    this.getZooKeeper().setData(zkPath + "/" + name + "/cur_server", null);
                    result = result + 1;
                }
            } else {
                result = result + 1;
            }
        }
        return result;
    }

    @Override
    public List<ScheduleServer> selectAllValidScheduleServer(String taskType) throws Exception {
        List<ScheduleServer> result = new ArrayList<ScheduleServer>();
        String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(taskType);
        String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType + "/" + taskType + "/" + this.PATH_Server;
        if (this.getZooKeeper().exists(zkPath, false) == null) {
            return result;
        }
        List<String> serverList = this.getZooKeeper().getChildren(zkPath);
        Collections.sort(serverList, new Comparator<String>() {
            @Override
            public int compare(String u1, String u2) {
                return u1.substring(u1.lastIndexOf("$") + 1).compareTo(u2.substring(u2.lastIndexOf("$") + 1));
            }
        });
        for (String name : serverList) {
            try {
                String valueString = this.getZooKeeper().getData(zkPath + "/" + name);
                ScheduleServer server = this.gson.fromJson(valueString, ScheduleServer.class);
                server.setCenterServerTime(new Timestamp(this.getSystemTime()));
                result.add(server);
            } catch (Exception e) {
                log.debug(e.getMessage(), e);
            }
        }
        return result;
    }

    @Override
    public List<String> loadScheduleServerNames(String taskType) throws Exception {
        String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(taskType);
        String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType + "/" + taskType + "/" + this.PATH_Server;
        if (this.getZooKeeper().exists(zkPath, false) == null) {
            return new ArrayList<String>();
        }
        List<String> serverList = this.getZooKeeper().getChildren(zkPath);
        Collections.sort(serverList, new Comparator<String>() {
            public int compare(String u1, String u2) {
                return u1.substring(u1.lastIndexOf("$") + 1).compareTo(u2.substring(u2.lastIndexOf("$") + 1));
            }
        });
        return serverList;
    }

    @Override
    public void assignTaskItem(String taskType, String currentUuid, int maxNumOfOneServer, List<String> taskServerList) throws Exception {
        if (this.isLeader(currentUuid, taskServerList) == false) {
            if (log.isDebugEnabled()) {
                log.debug(currentUuid + ":不是负责任务分配的Leader,直接返回");
            }
            return;
        }
        if (log.isDebugEnabled()) {
            log.debug(currentUuid + ":开始重新分配任务......");
        }
        if (taskServerList.size() <= 0) {
            // 在服务器动态调整的时候，可能出现服务器列表为空的清空
            return;
        }
        String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(taskType);
        String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType + "/" + taskType + "/" + this.PATH_TaskItem;
        List<String> children = this.getZooKeeper().getChildren(zkPath);
        // Collections.sort(children);
        // 20150323 有些任务分片，业务方其实是用数字的字符串排序的。优先以数字进行排序，否则以字符串排序
        children = listSort(children);
        int unModifyCount = 0;
        int[] taskNums = ScheduleUtil.assignTaskNumber(taskServerList.size(), children.size(), maxNumOfOneServer);
        int point = 0;
        int count = 0;
        String NO_SERVER_DEAL = "没有分配到服务器";
        for (int i = 0; i < children.size(); i++) {
            String name = children.get(i);
            if (point < taskServerList.size() && i >= count + taskNums[point]) {
                count = count + taskNums[point];
                point = point + 1;
            }
            String serverName = NO_SERVER_DEAL;
            if (point < taskServerList.size()) {
                serverName = taskServerList.get(point);
            }
            String curServerValue = this.getZooKeeper().getData(zkPath + "/" + name + "/cur_server");
            String reqServerValue = this.getZooKeeper().getData(zkPath + "/" + name + "/req_server");

            if (curServerValue == null || new String(curServerValue).equals(NO_SERVER_DEAL)) {
                this.getZooKeeper().setData(zkPath + "/" + name + "/cur_server", serverName);
                this.getZooKeeper().setData(zkPath + "/" + name + "/req_server", null);
            } else if (curServerValue.equals(serverName) == true && reqServerValue == null) {
                // 不需要做任何事情
                unModifyCount = unModifyCount + 1;
            } else {
                this.getZooKeeper().setData(zkPath + "/" + name + "/req_server", serverName);
            }
        }

        if (unModifyCount < children.size()) { // 设置需要所有的服务器重新装载任务
            log.info("设置需要所有的服务器重新装载任务:updateReloadTaskItemFlag......" + taskType + "  ,currentUuid " + currentUuid);

            this.updateReloadTaskItemFlag(taskType);
        }
        if (log.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            for (ScheduleTaskItem taskItem : this.loadAllTaskItem(taskType)) {
                buffer.append("\n").append(taskItem.toString());
            }
            log.debug(buffer.toString());
        }
    }

    @Override
    public boolean refreshScheduleServer(ScheduleServer server) throws Exception {
        Timestamp heartBeatTime = new Timestamp(this.getSystemTime());
        String zkPath = this.PATH_BaseTaskType + "/" + server.getBaseTaskType() + "/" + server.getTaskType() + "/"
                + this.PATH_Server + "/" + server.getUuid();
        if (this.getZooKeeper().exists(zkPath, false) == null) {
            // 数据可能被清除，先清除内存数据后，重新注册数据
            server.setRegister(false);
            return false;
        } else {
            Timestamp oldHeartBeatTime = server.getHeartBeatTime();
            server.setHeartBeatTime(heartBeatTime);
            server.setVersion(server.getVersion() + 1);
            String valueString = this.gson.toJson(server);
            try {
                this.getZooKeeper().setData(zkPath, valueString);
            } catch (Exception e) {
                // 恢复上次的心跳时间
                server.setHeartBeatTime(oldHeartBeatTime);
                server.setVersion(server.getVersion() - 1);
                throw e;
            }
            return true;
        }
    }

    @Override
    public void registerScheduleServer(ScheduleServer server) throws Exception {
        if (server.isRegister() == true) {
            throw new Exception(server.getUuid() + " 被重复注册");
        }
        String zkPath = this.PATH_BaseTaskType + "/" + server.getBaseTaskType() + "/" + server.getTaskType();
        if (this.getZooKeeper().exists(zkPath, false) == null) {
            this.getZooKeeper().createNode(zkPath, CreateMode.PERSISTENT, this.zkManager.getAcl());
        }
        zkPath = zkPath + "/" + this.PATH_Server;
        if (this.getZooKeeper().exists(zkPath, false) == null) {
            this.getZooKeeper().createNode(zkPath, CreateMode.PERSISTENT, this.zkManager.getAcl());
        }
        String realPath = null;
        // 此处必须增加UUID作为唯一性保障
        String zkServerPath =
                zkPath + "/" + server.getTaskType() + "$" + server.getIp() + "$" + (UUID.randomUUID().toString()
                        .replaceAll("-", "").toUpperCase()) + "$";
        realPath = this.getZooKeeper()
                .createNode(zkServerPath, CreateMode.PERSISTENT_SEQUENTIAL, this.zkManager.getAcl());
        server.setUuid(realPath.substring(realPath.lastIndexOf("/") + 1));

        Timestamp heartBeatTime = new Timestamp(this.getSystemTime());
        server.setHeartBeatTime(heartBeatTime);

        String valueString = this.gson.toJson(server);
        this.getZooKeeper().setData(realPath, valueString);
        server.setRegister(true);
    }

    @Override
    public void unRegisterScheduleServer(String taskType, String serverUUID) throws Exception {
        String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(taskType);
        String zkPath =
                this.PATH_BaseTaskType + "/" + baseTaskType + "/" + taskType + "/" + this.PATH_Server + "/" + serverUUID;
        if (this.getZooKeeper().exists(zkPath, false) != null) {
            this.getZooKeeper().deleteTree(zkPath);
        }
    }

    @Override
    public void clearExpireTaskTypeRunningInfo(String baseTaskType, String serverUUID, double expireDateInternal) throws Exception {
        for (String name : this.getZooKeeper().getChildren(this.PATH_BaseTaskType + "/" + baseTaskType)) {
            String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType + "/" + name + "/" + this.PATH_TaskItem;
            Stat stat = this.getZooKeeper().exists(zkPath, false);
            if (stat == null || getSystemTime() - stat.getMtime() > (long) (expireDateInternal * 24 * 3600 * 1000)) {
                this.getZooKeeper().deleteTree(this.PATH_BaseTaskType + "/" + baseTaskType + "/" + name);
            }
        }
    }

    @Override
    public boolean isLeader(String uuid, List<String> serverList) {
        return uuid.equals(getLeader(serverList));
    }

    @Override
    public void pauseAllServer(String baseTaskType) throws Exception {
        ScheduleTaskType taskType = this.loadTaskTypeBaseInfo(baseTaskType);
        taskType.setSts(ScheduleTaskType.STS_PAUSE);
        this.updateBaseTaskType(taskType);
    }

    @Override
    public void resumeAllServer(String baseTaskType) throws Exception {
        ScheduleTaskType taskType = this.loadTaskTypeBaseInfo(baseTaskType);
        taskType.setSts(ScheduleTaskType.STS_RESUME);
        this.updateBaseTaskType(taskType);
    }

    @Override
    public List<ScheduleTaskType> getAllTaskTypeBaseInfo() throws Exception {
        String zkPath = this.PATH_BaseTaskType;
        List<ScheduleTaskType> result = new ArrayList<ScheduleTaskType>();
        List<String> names = this.getZooKeeper().getChildren(zkPath);
        Collections.sort(names);
        for (String name : names) {
            result.add(this.loadTaskTypeBaseInfo(name));
        }
        return result;
    }

    @Override
    public void clearTaskType(String baseTaskType) throws Exception {
        // 清除所有的Runtime TaskType
        String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType;
        List<String> list = this.getZooKeeper().getChildren(zkPath);
        for (String name : list) {
            this.getZooKeeper().deleteTree(zkPath + "/" + name);
        }
    }

    @Override
    public void createBaseTaskType(ScheduleTaskType baseTaskType) throws Exception {
        if (baseTaskType.getBaseTaskType().indexOf("$") > 0) {
            throw new Exception("调度任务" + baseTaskType.getBaseTaskType() + "名称不能包括特殊字符 $");
        }
        String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType.getBaseTaskType();
        String valueString = this.gson.toJson(baseTaskType);
        if (this.getZooKeeper().exists(zkPath, false) == null) {
            this.getZooKeeper().createNode(zkPath, CreateMode.PERSISTENT, this.zkManager.getAcl(), valueString);
        } else {
            throw new Exception(
                    "调度任务" + baseTaskType.getBaseTaskType() + "已经存在,如果确认需要重建，请先调用deleteTaskType(String baseTaskType)删除");
        }
    }

    @Override
    public void updateBaseTaskType(ScheduleTaskType baseTaskType) throws Exception {
        if (baseTaskType.getBaseTaskType().indexOf("$") > 0) {
            throw new Exception("调度任务" + baseTaskType.getBaseTaskType() + "名称不能包括特殊字符 $");
        }
        String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType.getBaseTaskType();
        String valueString = this.gson.toJson(baseTaskType);
        if (this.getZooKeeper().exists(zkPath, false) == null) {
            this.getZooKeeper().createNode(zkPath, CreateMode.PERSISTENT, this.zkManager.getAcl(), valueString);
        } else {
            this.getZooKeeper().setData(zkPath, valueString);
        }
    }

    @Override
    public List<ScheduleTaskTypeRunningInfo> getAllTaskTypeRunningInfo(String baseTaskType) throws Exception {
        List<ScheduleTaskTypeRunningInfo> result = new ArrayList<ScheduleTaskTypeRunningInfo>();
        String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType;
        if (this.getZooKeeper().exists(zkPath, false) == null) {
            return result;
        }
        List<String> list = this.getZooKeeper().getChildren(zkPath);
        Collections.sort(list);

        for (String name : list) {
            ScheduleTaskTypeRunningInfo info = new ScheduleTaskTypeRunningInfo();
            info.setBaseTaskType(baseTaskType);
            info.setTaskType(name);
            info.setOwnSign(ScheduleUtil.splitOwnsignFromTaskType(name));
            result.add(info);
        }
        return result;
    }

    @Override
    public void deleteTaskType(String baseTaskType) throws Exception {
        this.getZooKeeper().deleteTree(this.PATH_BaseTaskType + "/" + baseTaskType);
    }

    @Override
    public List<ScheduleServer> selectScheduleServer(String baseTaskType, String ownSign, String ip, String orderStr) throws Exception {
        List<String> names = new ArrayList<String>();
        if (baseTaskType != null && ownSign != null) {
            names.add(baseTaskType + "$" + ownSign);
        } else if (baseTaskType != null && ownSign == null) {
            if (this.getZooKeeper().exists(this.PATH_BaseTaskType + "/" + baseTaskType, false) != null) {
                for (String name : this.getZooKeeper()
                        .getChildren(this.PATH_BaseTaskType + "/" + baseTaskType)) {
                    names.add(name);
                }
            }
        } else if (baseTaskType == null) {
            for (String name : this.getZooKeeper().getChildren(this.PATH_BaseTaskType)) {
                if (ownSign != null) {
                    names.add(name + "$" + ownSign);
                } else {
                    for (String str : this.getZooKeeper().getChildren(this.PATH_BaseTaskType + "/" + name)) {
                        names.add(str);
                    }
                }
            }
        }
        List<ScheduleServer> result = new ArrayList<ScheduleServer>();
        for (String name : names) {
            List<ScheduleServer> tempList = this.selectAllValidScheduleServer(name);
            if (ip == null) {
                result.addAll(tempList);
            } else {
                for (ScheduleServer server : tempList) {
                    if (ip.equals(server.getIp())) {
                        result.add(server);
                    }
                }
            }
        }
        Collections.sort(result, new ScheduleServerComparator(orderStr));
        // 排序
        return result;
    }

    @Override
    public List<ScheduleServer> selectHistoryScheduleServer(String baseTaskType, String ownSign, String ip, String orderStr) throws Exception {
        throw new Exception("没有实现的方法");
    }

    @Override
    public List<ScheduleServer> selectScheduleServerByManagerFactoryUUID(String factoryUUID) throws Exception {
        List<ScheduleServer> result = new ArrayList<ScheduleServer>();
        for (String baseTaskType : this.getZooKeeper().getChildren(this.PATH_BaseTaskType)) {
            for (String taskType : this.getZooKeeper()
                    .getChildren(this.PATH_BaseTaskType + "/" + baseTaskType)) {
                String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType + "/" + taskType + "/" + this.PATH_Server;
                for (String uuid : this.getZooKeeper().getChildren(zkPath)) {
                    String valueString = this.getZooKeeper().getData(zkPath + "/" + uuid);
                    ScheduleServer server = this.gson.fromJson(valueString, ScheduleServer.class);
                    server.setCenterServerTime(new Timestamp(this.getSystemTime()));
                    if (server.getManagerFactoryUUID().equals(factoryUUID)) {
                        result.add(server);
                    }
                }
            }
        }
        Collections.sort(result, new Comparator<ScheduleServer>() {
            @Override
            public int compare(ScheduleServer u1, ScheduleServer u2) {
                int result = u1.getTaskType().compareTo(u2.getTaskType());
                if (result == 0) {
                    String s1 = u1.getUuid();
                    String s2 = u2.getUuid();
                    result = s1.substring(s1.lastIndexOf("$") + 1).compareTo(s2.substring(s2.lastIndexOf("$") + 1));
                }
                return result;
            }
        });
        return result;
    }

    @Override
    public void createScheduleTaskItem(ScheduleTaskItem[] taskItems) throws Exception {
        for (ScheduleTaskItem taskItem : taskItems) {
            String zkPath =
                    this.PATH_BaseTaskType + "/" + taskItem.getBaseTaskType() + "/" + taskItem.getTaskType() + "/"
                            + this.PATH_TaskItem;
            if (this.getZooKeeper().exists(zkPath, false) == null) {
                this.getZooKeeper().createNode(zkPath, CreateMode.PERSISTENT, this.zkManager.getAcl());
            }
            String zkTaskItemPath = zkPath + "/" + taskItem.getTaskItem();
            this.getZooKeeper()
                    .createNode(zkTaskItemPath, CreateMode.PERSISTENT, this.zkManager.getAcl());
            this.getZooKeeper()
                    .createNode(zkTaskItemPath + "/cur_server", CreateMode.PERSISTENT, this.zkManager.getAcl());
            this.getZooKeeper()
                    .createNode(zkTaskItemPath + "/req_server", CreateMode.PERSISTENT, this.zkManager.getAcl());
            this.getZooKeeper()
                    .createNode(zkTaskItemPath + "/sts", CreateMode.PERSISTENT, this.zkManager.getAcl(), taskItem.getSts().toString());
            this.getZooKeeper()
                    .createNode(zkTaskItemPath + "/parameter", CreateMode.PERSISTENT, this.zkManager.getAcl(), taskItem.getDealParameter());
            this.getZooKeeper()
                    .createNode(zkTaskItemPath + "/deal_desc", CreateMode.PERSISTENT, this.zkManager.getAcl(), taskItem.getDealDesc());
        }
    }

    @Override
    public void updateScheduleTaskItemStatus(String taskType, String taskItem, ScheduleTaskItem.TaskItemSts sts, String message) throws Exception {
        String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(taskType);
        String zkPath =
                this.PATH_BaseTaskType + "/" + baseTaskType + "/" + taskType + "/" + this.PATH_TaskItem + "/" + taskItem;
        if (this.getZooKeeper().exists(zkPath + "/sts", false) == null) {
            this.getZooKeeper().setData(zkPath + "/sts", sts.toString());
        }
        if (this.getZooKeeper().exists(zkPath + "/deal_desc", false) == null) {
            if (message == null) {
                message = "";
            }
            this.getZooKeeper().setData(zkPath + "/deal_desc", message);
        }
    }

    @Override
    public void deleteScheduleTaskItem(String taskType, String taskItem) throws Exception {
        String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(taskType);
        String zkPath =
                this.PATH_BaseTaskType + "/" + baseTaskType + "/" + taskType + "/" + this.PATH_TaskItem + "/" + taskItem;
        this.getZooKeeper().deleteTree(zkPath);
    }

    @Override
    public void initialRunningInfo4Static(String baseTaskType, String ownSign, String uuid) throws Exception {
        String taskType = ScheduleUtil.getTaskTypeByBaseAndOwnSign(baseTaskType, ownSign);
        // 清除所有的老信息，只有leader能执行此操作
        String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType + "/" + taskType + "/" + this.PATH_TaskItem;
        try {
            this.getZooKeeper().deleteTree(zkPath);
        } catch (Exception e) {
            // 需要处理zookeeper session过期异常
            if (e instanceof KeeperException
                    && ((KeeperException) e).code().intValue() == KeeperException.Code.SESSIONEXPIRED.intValue()) {
                log.warn("delete : zookeeper session已经过期，需要重新连接zookeeper");
                zkManager.connect();
                this.getZooKeeper().deleteTree(zkPath);
            }
        }
        // 创建目录
        this.getZooKeeper().createNode(zkPath, CreateMode.PERSISTENT, this.zkManager.getAcl());
        // 创建静态任务
        this.createScheduleTaskItem(baseTaskType, ownSign, this.loadTaskTypeBaseInfo(baseTaskType).getTaskItems());
        // 标记信息初始化成功
        setInitialRunningInfoSucuss(baseTaskType, taskType, uuid);
    }

    /**
     * 根据基础配置里面的任务项来创建各个域里面的任务项
     */
    private void createScheduleTaskItem(String baseTaskType, String ownSign, String[] baseTaskItems) throws Exception {
        ScheduleTaskItem[] taskItems = new ScheduleTaskItem[baseTaskItems.length];
        Pattern p = Pattern.compile("\\s*:\\s*\\{");

        for (int i = 0; i < baseTaskItems.length; i++) {
            taskItems[i] = new ScheduleTaskItem();
            taskItems[i].setBaseTaskType(baseTaskType);
            taskItems[i].setTaskType(ScheduleUtil.getTaskTypeByBaseAndOwnSign(baseTaskType, ownSign));
            taskItems[i].setOwnSign(ownSign);
            Matcher matcher = p.matcher(baseTaskItems[i]);
            if (matcher.find()) {
                taskItems[i].setTaskItem(baseTaskItems[i].substring(0, matcher.start()).trim());
                taskItems[i]
                        .setDealParameter(baseTaskItems[i].substring(matcher.end(), baseTaskItems[i].length() - 1).trim());
            } else {
                taskItems[i].setTaskItem(baseTaskItems[i]);
            }
            taskItems[i].setSts(ScheduleTaskItem.TaskItemSts.ACTIVTE);
        }
        createScheduleTaskItem(taskItems);
    }


    @Override
    public void initialRunningInfo4Dynamic(String baseTaskType, String ownSign) throws Exception {
        String taskType = ScheduleUtil.getTaskTypeByBaseAndOwnSign(baseTaskType, ownSign);
        // 清除所有的老信息，只有leader能执行此操作
        String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType + "/" + taskType;
        if (this.getZooKeeper().exists(zkPath, false) == null) {
            this.getZooKeeper().createNode(zkPath, CreateMode.PERSISTENT, this.zkManager.getAcl());
        }
    }

    @Override
    public boolean isInitialRunningInfoSucuss(String baseTaskType, String ownSign) throws Exception {
        String taskType = ScheduleUtil.getTaskTypeByBaseAndOwnSign(baseTaskType, ownSign);
        String leader = this.getLeader(this.loadScheduleServerNames(taskType));
        String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType + "/" + taskType + "/" + this.PATH_TaskItem;
        if (this.getZooKeeper().exists(zkPath, false) != null) {
            String curContent = this.getZooKeeper().getData(zkPath);
            if (curContent != null && curContent.equals(leader)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void setInitialRunningInfoSucuss(String baseTaskType, String taskType, String uuid) throws Exception {
        String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType + "/" + taskType + "/" + this.PATH_TaskItem;
        this.getZooKeeper().setData(zkPath, uuid);
    }

    @Override
    public String getLeader(List<String> serverList) {
        if (serverList == null || serverList.size() == 0) {
            return "";
        }
        long no = Long.MAX_VALUE;
        long tmpNo = -1;
        String leader = null;
        for (String server : serverList) {
            tmpNo = Long.parseLong(server.substring(server.lastIndexOf("$") + 1));
            if (no > tmpNo) {
                no = tmpNo;
                leader = server;
            }
        }
        return leader;
    }

    @Override
    public long updateReloadTaskItemFlag(String taskType) throws Exception {
        String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(taskType);
        String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType + "/" + taskType + "/" + this.PATH_Server;
        Stat stat = this.getZooKeeper().setData(zkPath, "reload=true");
        return stat.getVersion();
    }

    @Override
    public long getReloadTaskItemFlag(String taskType) throws Exception {
        String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(taskType);
        String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType + "/" + taskType + "/" + this.PATH_Server;
        Stat stat = new Stat();
        this.getZooKeeper().getData(zkPath, stat);
        return stat.getVersion();
    }

    @Override
    public Map<String, Stat> getCurrentServerStatList(String taskType) throws Exception {
        Map<String, Stat> statMap = new HashMap<String, Stat>();
        String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(taskType);
        String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType + "/" + taskType + "/" + this.PATH_Server;
        List<String> childs = this.getZooKeeper().getChildren(zkPath);
        for (String serv : childs) {
            String singleServ = zkPath + "/" + serv;
            Stat servStat = this.getZooKeeper().exists(singleServ, false);
            statMap.put(serv, servStat);
        }
        return statMap;
    }

    private List<String> listSort(List<String> list) {
        Collections.sort(list, (u1, u2) -> {
            if (StringUtils.isNumeric(u1) && StringUtils.isNumeric(u2)) {
                int iU1 = Integer.parseInt(u1);
                int iU2 = Integer.parseInt(u2);
                if (iU1 == iU2) {
                    return 0;
                } else if (iU1 > iU2) {
                    return 1;
                } else {
                    return -1;
                }
            } else {
                return u1.compareTo(u2);
            }
        });
        return list;
    }
}
