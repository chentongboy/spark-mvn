package com.kong.learning.acm;

import java.util.SortedSet;
import java.util.TreeSet;
/**
 * 第一是如何快速搜索是否存在于某个数差值在t以内的数，可以使用TreeSet的subSet函数，可以返回在TreeSet中从
 * 在[lower,upper)的数，注意是前闭后开，这个函数返回的是SortedSet接口类型，TreeSet基于BST实现，特别适合
 * 于某个range里面的数的快速查找，下面的算法复杂度是O（nlogk）。第二，和Contains Duplicate的前面两题一样，
 * 有“the difference between i and j is at most k.”这样的限定的题目我们可以考虑维护一个长度为k的滑动窗口。
 * 第三，如果用Integer类型，这题有个test case会使得nums[i] + t + 1溢出变成负数，因此需要使用Long类型来保
 * 存数，要注意类型转换。
 * @author kong
 *
 */
public class Location {

	public boolean containsNearByAlmostDuplicate(int[] nums, int k, int t) {
		if (nums == null || nums.length < 2 || k < 1 || t < 0)
			return false;
		SortedSet<Long> windowNumSet = new TreeSet<Long>();
		for (int i = 0; i < nums.length; i++) {
			SortedSet<Long> set = windowNumSet.subSet((long) nums[i] - t, (long) nums[i] + t + 1);
			if (!set.isEmpty())
				return true;
			if (i >= k)
				windowNumSet.remove((long) nums[i - k]);
			windowNumSet.add((long) nums[i]);
		}
		return false;
	}
}
