package com.kong.learning.thread;

public class Person {

	private String left;
	private String right;
	
	public Person(String left, String right) {
		super();
		this.left = left;
		this.right = right;
	}

	/**
	 * left
	 * @return left
	 */
	public String getLeft() {
		return left;
	}

	/**
	 * left
	 * @param left
	 */
	public void setLeft(String left) {
		this.left = left;
	}

	/**
	 * right
	 * @return right
	 */
	public String getRight() {
		return right;
	}

	/**
	 * right
	 * @param right
	 */
	public void setRight(String right) {
		this.right = right;
	}
	
	public static void main(String[] args) {
		int i=1;
		while(i>0){
			i++;
			if (i==100) {
				return;
			}
			System.out.println(i);
		}
		System.out.println(i);
	}
}
