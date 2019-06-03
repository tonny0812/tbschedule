package com.keepgulp.taobaoschedulelearn.zk;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;

import java.io.Serializable;

/**
 * 配置信息
 */
@Data
@NoArgsConstructor
public class ConfigNode implements Serializable {

    private String rootPath;

    private String configType;

    private String name;

    private String value;

    public ConfigNode(String rootPath, String configType, String name) {
        this.rootPath = rootPath;
        this.configType = configType;
        this.name = name;
    }

    @Override
    public String toString() {
        StringBuffer buffer = new StringBuffer();
        buffer.append("配置根目录：").append(rootPath).append("\n");
        buffer.append("配置类型：").append(configType).append("\n");
        buffer.append("任务名称：").append(name).append("\n");
        buffer.append("配置的值：").append(value).append("\n");
        return buffer.toString();
    }
}
