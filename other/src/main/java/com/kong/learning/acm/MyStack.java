package com.kong.learning.acm;

import java.util.LinkedList;
/**
 * 用队列来模拟栈及栈的操作
 * 
 * @author kong
 * 
 */
public class MyStack {

	// 队列
	LinkedList<Integer> q1 = new LinkedList<Integer>();
	LinkedList<Integer> q2 = new LinkedList<Integer>();

	/**
	 *  入栈
	 * @param x
	 */
	public void push(int x) {
		q1.add(x);
	}

	/**
	 *  出栈
	 */
	public void pop() {
		while (q1.size() > 1) {
			q2.add(q1.poll());
		}
		q1.poll();
		LinkedList<Integer> tem = q1;
		q1 = q2;
		q2 = tem;
	}

	/**
	 * 取栈的最高的数
	 * @return
	 */
	public int top() {
		while (q1.size() > 1) {
			q2.add(q1.poll());
		}

		int result = q1.peek();
		q2.add(q1.poll());
		LinkedList<Integer> tem = q1;
		q1 = q2;
		q2 = tem;
		return result;
	}
	
	/**
	 * 栈是否为空
	 * @return
	 */
	public boolean empty() {
		return q1.isEmpty();
	}
	
}
