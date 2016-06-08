package com.kong.learning.acm;

import java.util.ArrayList;
import java.util.List;
/**
 * 找两个candidate和验证candidate出现的次数是否超过⌊ n/3 ⌋ times。
 * 找两个候选candidate的方法就是Moore投票法
 * @author kong
 *
 */
public class Majority {

	public List<Integer> majorityElement(int[] nums) {
		// moore voting
		// 0632
		int candidate1 = 0, candidate2 = 0, times1 = 0, times2 = 0;
		int n = nums.length;
		for (int i = 0; i < n; i++) {
			if (nums[i] == candidate1) {
				times1++;
			} else if (nums[i] == candidate2) {
				times2++;
			} else if (times1 == 0) {
				candidate1 = nums[i];
				times1 = 1;
			} else if (times2 == 0) {
				candidate2 = nums[i];
				times2 = 1;
			} else {
				times1--;
				times2--;
			}
		}

		times1 = 0;
		times2 = 0;
		for (int i = 0; i < n; i++) {
			if (nums[i] == candidate1)
				times1++;
			else if (nums[i] == candidate2)
				times2++;
		}

		List<Integer> res = new ArrayList<Integer>();
		if (times1 > n / 3) {
			res.add(candidate1);
		}
		if (times2 > n / 3) {
			res.add(candidate2);
		}
		return res;
		// 0649
	}
	/**
	 * 求主元素的经典算法是Moore投票算法，基本可以理解为将主元素和不同元素一一对应相消，
	 * 最后剩下来的元素就是主元素。具体做法是，初始计数器times和candidate都是0,然后遍历数组，
	 * 当times为0时置换新的candidate，遇到和candidate相同元素times++，遇到和candidate相异元素times--，
	 * 如此操作，如果主元素存在，最后剩下的一定是主元素。时间复杂度O（n）,空间复杂度O（1）.
	 * @param nums
	 * @return
	 */
	 public int majorityElement2(int[] nums) {  
	        //0627  
	        //Moore Voting   
	        int n = nums.length;  
	          
	        int candidate = 0;  
	        int times = 0;  
	        for(int i = 0; i < n; i++){  
	            if(times == 0) candidate = nums[i];  
	            if(nums[i] != candidate){  
	                times--;  
	            } else{  
	                times++;  
	            }  
	        }  
	        return candidate;  
	        //0631  
	    }  
}
