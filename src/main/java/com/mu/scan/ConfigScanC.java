package com.mu.scan;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

/**
 * 扫描组件
 */
@Configuration
@ComponentScan(basePackages = "com.mu.bean")
public class ConfigScanC {

    public ConfigScanC() {
        System.out.println("ConfigScanC容器启动初始化。。。");
    }
}
