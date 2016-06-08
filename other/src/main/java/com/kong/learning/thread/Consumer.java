package com.kong.learning.thread;

public class Consumer implements Runnable{

	private Info info = null;
	
	public Consumer(Info info){
		this.info=info;
	}
	
	@Override
	public void run() {
		for (int i = 0; i < 10; i++) {
			System.out.println("我消费了");
			this.info.get();
		}
		
	}

}
