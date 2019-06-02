package com.keepgulp.taobaoschedulelearn.taskdefine;

import lombok.Data;

/**
 * 任务队列类型
 *
 * @author xuannan
 */
@Data
public class ScheduleTaskItem {

    public enum TaskItemSts {
        ACTIVTE, FINISH, HALT
    }

    /**
     * 处理任务类型
     */
    private String taskType;

    /**
     * 原始任务类型
     */
    private String baseTaskType;

    /**
     * 完成状态
     */
    private TaskItemSts sts = TaskItemSts.ACTIVTE;

    /**
     * 任务处理需要的参数
     */
    private String dealParameter = "";

    /**
     * 任务处理情况,用于任务处理器会写一些信息
     */
    private String dealDesc = "";

    /**
     * 队列的环境标识
     */
    private String ownSign;

    /**
     * 任务队列ID
     */
    private String taskItem;
    /**
     * 持有当前任务队列的任务处理器
     */
    private String currentScheduleServer;
    /**
     * 正在申请此任务队列的任务处理器
     */
    private String requestScheduleServer;

    /**
     * 数据版本号
     */
    private long version;


    @Override
    public String toString() {
        return "TASK_TYPE=" + this.taskType + ":TASK_ITEM=" + this.taskItem + ":CUR_SERVER="
                + this.currentScheduleServer + ":REQ_SERVER=" + this.requestScheduleServer
                + ":DEAL_PARAMETER=" + this.dealParameter;
    }

}