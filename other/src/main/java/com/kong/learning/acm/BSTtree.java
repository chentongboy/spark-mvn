package com.kong.learning.acm;

import java.util.ArrayList;
import java.util.List;

/**
 * 卡特兰数与树： h(n)=h(0)*h(n-1)+h(1)*h(n-2)+h(2)*h(n-3)+...+h(n-1)*h(0)
 * 
 * @author kong
 * 
 */
public class BSTtree {

	/**
	 * 卡特兰数
	 * 
	 * @param n
	 * @return
	 */
	public int numTrees(int n) {
		if (n <= 0)
			return 0;
		int[] C = new int[n + 1];
		C[0] = 1;
		C[1] = 1;
		for (int i = 2; i <= n; i++) {
			for (int j = 0; j < i; j++) {
				C[i] += C[j] * C[i - j - 1];
			}
		}
		return C[n];
	}

	public static void main(String[] args) {

		int n = 3;
		BSTtree bsTtree = new BSTtree();
		List<Node> result = bsTtree.generateTrees(n);
		// int result = bsTtree.numTrees(n);
		// int result = bsTtree.trees(n);
		try {
			for (Node node : result) {
				System.out.println("根节点");
				bsTtree.print(node);
				bsTtree.isNodeNull(node);
			}
			System.out.println(result.size());
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("又错了");
		}

	}

	/**
	 * 内部类：节点
	 * 
	 * @author kong
	 * 
	 */
	private static class Node {
		Node leftChild;
		Node rightChild;
		int data;

		Node(int newData) {
			leftChild = null;
			rightChild = null;
			data = newData;
		}
	}

	/**
	 * 构造二叉树入口
	 * 
	 * @param n
	 * @return
	 */
	public List<Node> generateTrees(int n) {
		return helper(1, n);
	}

	/**
	 * 构造二叉树
	 * 卡特兰树
	 * @param left
	 * @param right
	 * @return
	 */
	private List<Node> helper(int left, int right) {
		List<Node> result = new ArrayList<Node>();
		if (left > right) {
			result.add(null);
			return result;
		}
		for (int i = left; i <= right; i++) {
			List<Node> leftList = helper(left, i - 1);
			List<Node> rigthList = helper(i + 1, right);
			for (int j = 0; j < leftList.size(); j++) {
				for (int k = 0; k < rigthList.size(); k++) {
					Node root = new Node(i);
					root.leftChild = leftList.get(j);
					root.rightChild = rigthList.get(k);
					result.add(root);
				}
			}
		}
		return result;
	}

	/**
	 * 打印节点值
	 * @param node
	 */
	public void print(Node node) {
		System.out.println(node.data);
	}
	
	/**
	 * 左右节点不为空都打印出来
	 * @param node
	 */
	public void isNodeNull(Node node) {
		if (node.leftChild != null) {
			System.out.println("左节点：");
			print(node.leftChild);
			isNodeNull(node.leftChild);
		}
		if (node.rightChild != null) {
			System.out.println("右节点：");
			print(node.rightChild);
			isNodeNull(node.rightChild);
		}
	}
}
