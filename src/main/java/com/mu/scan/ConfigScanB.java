package com.mu.scan;

import com.mu.bean.MyBean;
import com.mu.bean.MyBeanB;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 一个bean引入另一个bean
 */
@Configuration
public class ConfigScanB {

    public ConfigScanB() {
        System.out.println("<beans...> </beans>");
    }

    @Bean
    public MyBean beanName() {
        System.out.println("xml：\n<bean id='beanName' class='com.mu.bean.MyBean' />");
        return new MyBean();
    }

    @Bean
    public MyBeanB beanNameB() {
        System.out.println("xml：\n<bean id='beanNameB' class='com.mu.bean.MyBeanB'>\n\t<property name='beanName' ref='beanName'/>\n</bean>");
        MyBeanB mbB = new MyBeanB();
        mbB.setMyBean(beanName());
        return mbB;
    }
}
