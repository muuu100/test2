package com.mu;

import com.mu.bean.MyBean;
import com.mu.bean.MyBeanC;
import com.mu.scan.*;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class ConfigMain {

    public static void main(String[] args) {

        ApplicationContext context = new AnnotationConfigApplicationContext(ConfigScanE.class);
        MyBean mb = (MyBean) context.getBean("beanNameE");
        mb.sayHello();

        MyBean inner = (MyBean) context.getBean("beanNameInner");
        inner.sayHello();

        // ----------------@Import---------------------
//        ApplicationContext context = new AnnotationConfigApplicationContext(ConfigScanD.class);
//        MyBean mb = (MyBean) context.getBean("beanName");
//        mb.sayHello();

//        ApplicationContext context = new AnnotationConfigApplicationContext(ConfigScanD.class);
//        MyBeanC mbc = (MyBeanC) context.getBean("myBeanC");
//        mbc.sayHello();

        // -----------------@Component、@ComponentScan---------------
//        ApplicationContext context = new AnnotationConfigApplicationContext(ConfigScanC.class);
//        MyBeanC mbc = (MyBeanC) context.getBean("myBeanC");
//        mbc.sayHello();

        //--------------------------作用域---------------------------
//        ApplicationContext context = new AnnotationConfigApplicationContext(ConfigScan.class);
//        MyBean mb = (MyBean) context.getBean("beanName");
//        mb.sayHello();
//        System.out.println(mb);
//        MyBean mb2 = (MyBean) context.getBean("beanName");
//        mb2.sayHello();
//        System.out.println(mb2);


        //--------------------测试依赖bean----------------------------
//        ApplicationContext context = new AnnotationConfigApplicationContext(ConfigScanB.class);
//        MyBeanB mbB = (MyBeanB) context.getBean("beanNameB");
//        mbB.sayHelloB();


        //---------------------测试bean------------------------------------
        //获取bean
//        ApplicationContext context = new AnnotationConfigApplicationContext(ConfigScan.class);
//        MyBean mb = (MyBean) context.getBean("beanName");
//        mb.sayHello();
        //---------------------------------------------------------


        // @Configuration注解的spring容器加载方式，用AnnotationConfigApplicationContext替换ClassPathXmlApplicationContext
        // 如果加载spring-context.xml文件：
        // ApplicationContext context = new ClassPathXmlApplicationContext("spring-context.xml");
    }
}
