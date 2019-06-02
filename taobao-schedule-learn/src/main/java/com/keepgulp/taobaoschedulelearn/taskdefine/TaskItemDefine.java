package com.keepgulp.taobaoschedulelearn.taskdefine;

import lombok.Data;

/**
 * 任务定义，提供关键信息给使用者
 *
 * @author xuannan
 */
@Data
public class TaskItemDefine {

    /**
     * 任务项ID
     */
    private String taskItemId;
    /**
     * 任务项自定义参数
     */
    private String parameter;

    @Override
    public String toString() {
        return "(t=" + taskItemId + ",p=" + parameter + ")";
    }

}
