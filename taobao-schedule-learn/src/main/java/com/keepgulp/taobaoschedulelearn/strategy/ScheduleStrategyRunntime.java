package com.keepgulp.taobaoschedulelearn.strategy;

import com.keepgulp.taobaoschedulelearn.serverdefine.ScheduleStrategy;
import lombok.Data;

@Data
public class ScheduleStrategyRunntime {

    /**
     * 任务类型
     */
    String strategyName;
    String uuid;
    String ip;

    private ScheduleStrategy.Kind kind;

    /**
     * Schedule Name,Class Name、Bean Name
     */
    private String taskName;

    private String taskParameter;

    /**
     * 需要的任务数量
     */
    int requestNum;
    /**
     * 当前的任务数量
     */
    int currentNum;

    String message;

    @Override
    public String toString() {
        return "ScheduleStrategyRunntime [strategyName=" + strategyName + ", uuid=" + uuid + ", ip=" + ip + ", kind="
            + kind + ", taskName=" + taskName + ", taskParameter="
            + taskParameter + ", requestNum=" + requestNum + ", currentNum=" + currentNum + ", message=" + message
            + "]";
    }

}
