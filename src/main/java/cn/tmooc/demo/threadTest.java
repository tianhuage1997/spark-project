package cn.tmooc.demo;

public class threadTest {

    public static void main(String[] args) {

        T t1=new T();
        T t2=new T();

        t1.setName("t1");
        t2.setName("t2");

        t1.start();
        t2.start();

//        R r=new R();
//        Thread t1=new Thread(r);
//        Thread t2=new Thread(r);
//        t1.setName("t1");
//        t1.setName("t2");
//        t1.start();
//        t2.start();

    }
}
