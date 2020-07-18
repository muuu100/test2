package com.mu.scan;

import com.mu.bean.MyBean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * 引入spring的xml配置文件（未测）
 * 引入注解配置
 */
@Configuration
//@ImportResource("classpath:applicationContext-configuration.xml")
@Import(ConfigScanC.class)
public class ConfigScanD {
}
