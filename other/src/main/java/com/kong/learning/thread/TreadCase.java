package com.kong.learning.thread;

public class TreadCase {

	public static void main(String[] args) {
		Info info = new Info();
		Producer producer = new Producer(info);
		Consumer consumer = new Consumer(info);
		new Thread(producer).start();
		try {
			Thread.sleep(500);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		new Thread(consumer).start();
	}
}
