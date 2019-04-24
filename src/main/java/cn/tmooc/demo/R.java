package cn.tmooc.demo;

public class R   implements  Runnable{

    private  static  int count;

    @Override
    public  synchronized   void run(){
        for (int i=1;i<=5;i++){
            count++;
            try {
                System.out.println(Thread.currentThread().getName()+":"+count);
                Thread.sleep(500);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }
}
