package com.kong.learning.thread;

public class SleepInterrupt extends Object implements Runnable{

	public static void main(String[] args) {
		SleepInterrupt sleepInterrupt = new SleepInterrupt();
		Thread thread = new Thread(sleepInterrupt);
		thread.start();
		try {
			Thread.sleep(2000);
			System.out.println("in main()- sleep");
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		System.out.println("in main()-interrupt other thread");
		thread.interrupt();
		System.out.println("in main()-leaving");
	}

	@Override
	public void run() {
		try {
			System.out.println("in run()-about to sleep for 20 seconds");
			Thread.sleep(20000);
			System.out.println("in run()-woke up");
		} catch (InterruptedException e) {
			System.out.println("in run()-interrupted while sleeping");
			e.printStackTrace();
			//如果没有return，线程不会被中断，它会继续打印下面的信息
			return;
		}
		System.out.println("in run()-leaving normally");
	}

}
