package com.keepgulp.taobaoschedulelearn.serverdefine;

import lombok.Data;
import org.apache.commons.lang3.builder.ToStringBuilder;

@Data
public class ScheduleStrategy {

    public enum Kind {
        Schedule, Java, Bean
    }

    /**
     * 任务类型
     */
    private String strategyName;

    private String[] IPList;

    private int numOfSingleServer;
    /**
     * 指定需要执行调度的机器数量
     */
    private int assignNum;

    private Kind kind;

    /**
     * Schedule Name,Class Name、Bean Name
     */
    private String taskName;

    private String taskParameter;

    /**
     * 服务状态: pause,resume
     */
    private String sts = STS_RESUME;

    public static String STS_PAUSE = "pause";
    public static String STS_RESUME = "resume";

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
}
