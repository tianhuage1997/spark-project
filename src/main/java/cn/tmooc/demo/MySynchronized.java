package cn.tmooc.demo;

class MySynchronized  extends  Thread
{
    private int i=1;
    public void run()
    {
        synchronized(this)//this
        {
            for(; i<20; i++)
                System.out.println(Thread.currentThread().getName()+" "+i);
        }
    }
    public static void main(String[] args)
    {
        MySynchronized ms=new MySynchronized();
        MySynchronized ms2=new MySynchronized();


        new Thread(ms).start();
        new Thread(ms2).start();
//        new Thread(ms).start();

    }
}