package com.mu.bean;

import org.springframework.stereotype.Component;

/**
 * @Component
 */
@Component
public class MyBeanC {
    public MyBeanC() {
        System.out.println("<bean id='myBeanC' class='com.mu.bean.MyBeanC' />");
    }

    public void sayHello() {
        System.out.println("MyBeanC sayHello...");
    }

    public void start() {
        System.out.println("MyBeanC init。。。");
    }

    public void clean() {
        System.out.println("MyBeanC destroy。。。");
    }
}
