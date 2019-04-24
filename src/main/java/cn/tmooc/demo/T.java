package cn.tmooc.demo;

import groovy.transform.Synchronized;

import java.util.concurrent.atomic.AtomicInteger;

public   class  T  extends  Thread {

    private   static int count =0;
//    private AtomicInteger count = new AtomicInteger(0);

    @Override
    public  void  run(){
         synchronized (this){

                 for (int i =1;i<=5;i++){
                     count++;
//                     count.incrementAndGet();
                     try {
                         System.out.println(getName()+":"+count);
                         Thread.sleep(500);
                     }catch (Exception e){
                         e.printStackTrace();
                     }

             }
         }
    }
}
