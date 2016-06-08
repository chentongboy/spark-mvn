package com.kong.learning.acm;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 多叉树
 * 
 * @author kong
 * 
 */
public class TreeNodes implements Serializable {

	private static final long serialVersionUID = 1L;

	private int parentId;
	private int selfId;
	protected String nodeName;
	protected Object obj;
	protected TreeNodes parentNode;
	protected List<TreeNodes> childList;

	public TreeNodes() {
		intitChildList();
	}

	/**
	 * 初始化孩子列表
	 */
	public void intitChildList() {
		if (childList == null) {
			childList = new ArrayList<TreeNodes>();
		}
	}

	public TreeNodes(TreeNodes parentNode) {
		this.getParentNode();
		intitChildList();
	}

	/**
	 * 是否存在树节点
	 * @return
	 */
	public boolean isLeaf() {
		if (childList == null)
			return true;
		if (childList.isEmpty())
			return true;
		return false;
	}

	/**
	 * 增加child节点到当前节点中
	 * @param treeNodes
	 */
	public void addChildNode(TreeNodes treeNodes) {
		intitChildList();
		childList.add(treeNodes);
	}

	/**
	 * 是有效树
	 * @return
	 */
	public boolean isValidTree() {
		return true;
	}

	/**
	 * 返回当前节点的父辈节点集合
	 * @return
	 */
	public List<TreeNodes> getElders() {
		List<TreeNodes> elderList = new ArrayList<TreeNodes>();
		TreeNodes parentNode = this.getParentNode();
		if (parentNode == null)
			return elderList;
		elderList.add(parentNode);
		elderList.addAll(parentNode.getElders());
		return elderList;
	}

	/**
	 * 返回当前节点的晚辈集合
	 * @return
	 */
	public List<TreeNodes> getJuniors() {
		List<TreeNodes> juniorList = new ArrayList<TreeNodes>();
		List<TreeNodes> childList = this.getChildList();
		if (childList == null)
			return juniorList;
		int childNumber = childList.size();
		for (int i = 0; i < childNumber; i++) {
			TreeNodes junior = childList.get(i);
			juniorList.add(junior);
			juniorList.addAll(junior.getJuniors());
		}
		return juniorList;
	}

	/**
	 * 删除节点和它下面的晚辈
	 */
	public void deleteNode() {
		TreeNodes parentNode = this.getParentNode();
        int id = this.getSelfId();
        if (parentNode !=null) {
			parentNode.deleteChildNode(id);
		}
	}

	/**
	 * 删除当前节点的某个子节点
	 * @param childId
	 */
	private void deleteChildNode(int childId) {
		List<TreeNodes> childList = this.getChildList();
		int childNumber = childList.size();
		for (int i = 0; i < childNumber; i++) {
			TreeNodes child = childList.get(i);
			if (child.getSelfId() == childId) {
				childList.remove(i);
				return;
			}
		}
		
	}

	/**
	 * 动态插入一个新的节点到当前树中
	 * @param treeNodes
	 * @return
	 */
	public boolean insertJuniorNode(TreeNodes treeNodes) {
		int juniorParentId = treeNodes.getParentId();
		if (this.parentId == juniorParentId) {
			addChildNode(treeNodes);
			return true;
		}
		List<TreeNodes> childList = this.getChildList();
		int childNumber = childList.size();
		boolean insertFlag;
		for (int i = 0; i < childNumber; i++) {
			TreeNodes childNode = childList.get(i);
			insertFlag = childNode.insertJuniorNode(treeNodes);
			if (insertFlag)
				return true;
		}
		return false;
	}
	
	/**
	 * 找到一颗树中的某个节点
	 * @param id
	 * @return
	 */
	public TreeNodes findTreeNodeById(int id) {
		if (this.selfId == id)
			return this;
		if (childList.isEmpty() || childList == null)
			return null;
	    int childNumber =childList.size();
	    for (int i = 0; i < childNumber; i++) {
			TreeNodes child = childList.get(i);
			TreeNodes resultNode = child.findTreeNodeById(id);
			if(resultNode !=null)
				return resultNode;
		}
	    return null;
	}
	
	/**
	 *遍历一颗树，层次遍历
	 */
	public void traverse() {
		if(selfId<0)
			return;
		print(this.selfId);
		if (childList == null || childList.isEmpty())
			return;
		int childNumber = childList.size();
		for (int i = 0; i < childNumber; i++) {
			TreeNodes childNode = childList.get(i);
			childNode.traverse();
		}
	}
	
	public void print(int content) {
		System.out.println(String.valueOf(content));
		
	}
	
	public void print(String content) {
		System.out.println(content);
	}

	/**
	 * parentId
	 * 
	 * @return parentId
	 */
	public int getParentId() {
		return parentId;
	}

	/**
	 * parentId
	 * 
	 * @param parentId
	 */
	public void setParentId(int parentId) {
		this.parentId = parentId;
	}

	/**
	 * selfId
	 * 
	 * @return selfId
	 */
	public int getSelfId() {
		return selfId;
	}

	/**
	 * selfId
	 * 
	 * @param selfId
	 */
	public void setSelfId(int selfId) {
		this.selfId = selfId;
	}

	/**
	 * nodeName
	 * 
	 * @return nodeName
	 */
	public String getNodeName() {
		return nodeName;
	}

	/**
	 * nodeName
	 * 
	 * @param nodeName
	 */
	public void setNodeName(String nodeName) {
		this.nodeName = nodeName;
	}

	/**
	 * obj
	 * 
	 * @return obj
	 */
	public Object getObj() {
		return obj;
	}

	/**
	 * obj
	 * 
	 * @param obj
	 */
	public void setObj(Object obj) {
		this.obj = obj;
	}

	/**
	 * parentNode
	 * 
	 * @return parentNode
	 */
	public TreeNodes getParentNode() {
		return parentNode;
	}

	/**
	 * parentNode
	 * 
	 * @param parentNode
	 */
	public void setParentNode(TreeNodes parentNode) {
		this.parentNode = parentNode;
	}

	/**
	 * childList
	 * 
	 * @return childList
	 */
	public List<TreeNodes> getChildList() {
		return childList;
	}

	/**
	 * childList
	 * 
	 * @param childList
	 */
	public void setChildList(List<TreeNodes> childList) {
		this.childList = childList;
	}

}
