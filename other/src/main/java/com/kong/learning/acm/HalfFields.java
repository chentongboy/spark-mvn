package com.kong.learning.acm;
/**
 * 二分查找取最小值
 * @author kong
 *
 */
public class HalfFields {

	/**
	 * 时间复杂度为O(n)
	 * @param nums
	 * @return
	 */
	public int findMin(int[] nums) {
		int n = nums.length;
		int l = 0;
		int r = n - 1;
		int min = nums[0];
		while (l < r) {
			int m = l + (r - l) / 2;
			if (nums[m] < nums[l]) {
				min = Math.min(nums[m], min);
				r = m;
			} else if (nums[m] > nums[l]) {
				min = Math.min(nums[l], min);
				l = m;
			} else {
				l++;
			}
		}
		min = Math.min(nums[l], min);
		min = Math.min(nums[r], min);
		return min;
	}
	
	public static void main(String[] args) {
		int[] test = {8,4,1,2,3,4,5,6,7,8,9};
		HalfFields halfFields = new HalfFields();
		int min = halfFields.findMin(test);
		System.out.println(min);
	}
}
