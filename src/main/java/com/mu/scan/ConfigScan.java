package com.mu.scan;

import com.mu.bean.MyBean;
import com.mu.bean.MyBeanB;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

/**
 * 1、@Bean
 * 2、init、destroy、@Scope
 */
@Configuration
public class ConfigScan {

    public ConfigScan() {
        System.out.println("ConfigScan容器启动初始化。。。");
    }

    //    @Bean(name="beanName",initMethod="start",destroyMethod="clean")
//    @Scope("prototype")
    @Bean
    public MyBean beanName() {
        return new MyBean();
    }
}
