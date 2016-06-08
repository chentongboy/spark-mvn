package com.kong.learning.thread;
/**
 * 定义信息类
 * @author kong
 *
 */
public class Info {

	private String name="name";
	private String content ="content";
	private boolean flag = true;//设置标志位，初始化时先生产
	public synchronized void set(String name,String content) {
		while (!flag) {
			try {
				super.wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		this.setName(name);
		try {
			Thread.sleep(300);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		this.setContent(content);
		flag=false; //改变标志位，表示可以取走
		super.notify();
	}
	
	public synchronized void get(){
		while (flag) {
			try {
				super.wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		try {
			Thread.sleep(300);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println(this.getName()+"-->"+this.getContent());
		flag=true; //改变标志位，表示可以生产
		super.notify();
	}
	
	
	/**
	 * name
	 * @return name
	 */
	public String getName() {
		return name;
	}
	/**
	 * name
	 * @param name
	 */
	public void setName(String name) {
		this.name = name;
	}
	/**
	 * content
	 * @return content
	 */
	public String getContent() {
		return content;
	}
	/**
	 * content
	 * @param content
	 */
	public void setContent(String content) {
		this.content = content;
	}
	
	
}
