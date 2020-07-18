package com.mu.bean;

/**
 * 引入MyBean
 */
public class MyBeanB {

    private MyBean myBean;

    public void sayHelloB() {
        myBean.sayHello();
        System.out.println("MyBeanB sayHello...");
    }

    public MyBean getMyBean() {
        return myBean;
    }

    public void setMyBean(MyBean myBean) {
        this.myBean = myBean;
    }
}
