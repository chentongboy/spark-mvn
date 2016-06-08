package com.kong.learning.thread;

public class Producer implements Runnable{

	private Info info = null;
	
	public Producer(Info info) {
		this.info=info;
	}
	
	@Override
	public void run() {
		boolean flag = true;//定义标志位
		for (int i = 0; i < 10; i++) {
			if (flag) {
				System.out.println("开始生产");
				this.info.set("kong1", "123456");
				flag=false;
			}else {
				System.out.println("再生产");
				this.info.set("kong2", "142536");
				flag=true;
			}
		}
		
	}

}
