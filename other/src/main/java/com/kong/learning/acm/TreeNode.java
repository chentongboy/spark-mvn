package com.kong.learning.acm;

import java.util.LinkedList;
import java.util.List;

/**
 * 二叉树
 * 
 * @author kong
 * 
 */
public class TreeNode {

	private int[] array = {8,9};
	private static List<Node> nodeList = null;

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

	public void createBinTree() {
		nodeList = new LinkedList<Node>();
		// 将一个数组的值依次转换为Node节点
		for (int nodeIndex = 0; nodeIndex < array.length; nodeIndex++) {
			nodeList.add(new Node(array[nodeIndex]));
		}

		// 对前lastParentIndex-1个父节点按照父节点的数字关系建立二叉树
		for (int parentIndex = 0; parentIndex < array.length / 2 - 1; parentIndex++) {
			// 左孩子
			nodeList.get(parentIndex).leftChild = nodeList.get(parentIndex * 2 + 1);
			// 右孩子
			nodeList.get(parentIndex).rightChild = nodeList.get(parentIndex * 2 + 2);
		}

		// 最后一个父节点：因为最后一个父节点可能没有右孩子，所以单独拿出来
		int lastParentIndex = array.length / 2 - 1;
		// 左孩子
		nodeList.get(lastParentIndex).leftChild = nodeList.get(lastParentIndex * 2 + 1);
		// 右孩子，如果数组的长度为奇数才建立右孩子
		if (array.length % 2 == 1) {
			nodeList.get(lastParentIndex).rightChild = nodeList.get(lastParentIndex * 2 + 2);
		}
		
	}
	/**
	 * 前序遍历
	 * 
	 * @param node
	 */
	public static void preOrderTraverse(Node node) {
		if (node == null)
			return;
		System.out.println(node.data + " ");
		preOrderTraverse(node.leftChild);
		preOrderTraverse(node.rightChild);
	}

	/**
	 * 中序遍历
	 * 
	 * @param node
	 */
	public static void inOrderTraverse(Node node) {
		if (node == null)
			return;
		inOrderTraverse(node.leftChild);
		System.out.println(node.data + " ");
		inOrderTraverse(node.rightChild);
	}

	/**
	 * 后序遍历
	 * 
	 * @param node
	 */
	public static void postOrderTraverse(Node node) {
		if (node == null)
			return;
		postOrderTraverse(node.leftChild);
		postOrderTraverse(node.rightChild);
		System.out.println(node.data + " ");
	}

	public static void main(String[] args) {
		TreeNode treeNode = new TreeNode();
		treeNode.createBinTree();
		// nodeList中的第0个索引处的值即为根节点
		Node root = nodeList.get(0);

		System.out.println("先序遍历：");
		preOrderTraverse(root);
		System.out.println();

		System.out.println("中序遍历：");
		inOrderTraverse(root);
		System.out.println();

		System.out.println("后序遍历：");
		postOrderTraverse(root);
	}
}
