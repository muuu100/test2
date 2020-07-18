package com.mu.scan;

import com.mu.bean.MyBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * configuration嵌套
 */
@Configuration
public class ConfigScanE {

    @Bean
    public MyBean beanNameE() {
        return new MyBean();
    }

    @Configuration
    static class InnerConfig {
        @Bean
        MyBean beanNameInner() {
            return new MyBean();
        }
    }
}
