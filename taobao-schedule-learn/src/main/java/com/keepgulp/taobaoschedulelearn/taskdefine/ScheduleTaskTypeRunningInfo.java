package com.keepgulp.taobaoschedulelearn.taskdefine;

import lombok.Data;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.sql.Timestamp;

@Data
public class ScheduleTaskTypeRunningInfo {

    private long id;

    /**
     * 任务类型：原始任务类型+"-"+ownSign
     */
    private String taskType;

    /**
     * 原始任务类型
     */
    private String baseTaskType;

    /**
     * 环境
     */
    private String ownSign;

    /**
     * 最后一次任务分配的时间
     */
    private Timestamp lastAssignTime;

    /**
     * 最后一次执行任务分配的服务器
     */
    private String lastAssignUUID;

    private Timestamp gmtCreate;

    private Timestamp gmtModified;

    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }


}
