package com.kong.learning.full;

import java.util.ArrayList;
import java.util.List;
/**
 * 递归法，时间复杂度太复杂，无法计算大面积
 * @author kong
 *
 */
public class Half2 {

	int amax;
	int bmax;
	int cmax;
	boolean flag;
	List<Integer> intList = new ArrayList<Integer>();

	public Half2() {

	}
	
	public boolean isHalf(List<Integer> list) {
		if (list.size()>2) {
			intList = new ArrayList<Integer>();
			return true;
		}
		return false;
	}

	public void firstStep(int a, int b, int c, int first) {
		if (first == 0) {
			amax = a;
			bmax = b;
			cmax = c;
			first++;
			int d = a - b;
			b = 0;
			if (d > c) {
				c = 0;
			} else {
				b = c - d;
				c = 0;
			}
			if (a == (bmax + cmax) / 2) {
				intList.add(a);
				intList.add(b);
				intList.add(c);
				return;
			}
			firstStep(a, b, c, first);
		}

		if (a == amax) {
			boolean bl = true;
			while (bl) {
				a = a - cmax;
				c = cmax;
				if ((b + c) > bmax) {
					c = b + c - bmax;
					b = bmax;
				} else {
					b = b + c;
					c = 0;
				}
				if (a == ((bmax + cmax) / 2)) {
					intList.add(a);
					intList.add(b);
					intList.add(c);
					return;
				} else if (a < 0) {
					return;
				}
				if (b == bmax) {
					bl = false;
				}

			}
			firstStep(a, b, c, 1);
		} else {
			if (b != bmax) {
				a = a - cmax;
				c = cmax;
				if ((b + c) > bmax) {
					c = b + c - bmax;
					b = bmax;
				} else {
					b = b + c;
					c = 0;
				}
				if (a == ((bmax + cmax) / 2)) {
					intList.add(a);
					intList.add(b);
					intList.add(c);
					return;
				} else if (a < 0) {
					return;
				}
				firstStep(a, b, c, first);
			} else {

				if ((a + b) > amax) {
					b = a + b - amax;
					a = amax;
					if ((b + c) > bmax) {
						c = b + c - bmax;
						b = bmax;
					} else {
						b = b + c;
						c = 0;
					}
				} else {
					a = a + b;
					b = c;
					c = 0;
				}
				if (a == ((bmax + cmax) / 2)) {
					intList.add(a);
					intList.add(b);
					intList.add(c);
					return;
				} else if (a < 0) {
					return;
				}
				firstStep(a, b, c, first);
			}
		}

	}

	/**
	 * 容量设置检查
	 * 
	 * @param a
	 * @param b
	 * @param c
	 * @return true 正确 false 错误
	 */
	public boolean check(int a, int b, int c) {
		if (a < b || a < c || a == b || a == c || a > (b + c) || a == (b + c)) {
			// System.out.println("参数最大容量设置错误！");
		} else if (b < c || b == c) {
			// System.out.println("参数中等容量与最小容量设置错误！");
		} else if ((b + c) % 2 != 0) {
			// System.out.println("请输入奇数！");
		} else {
			return true;
		}
		return false;
	}

	public static void main(String[] args) {
		int a = 19, b = 13, c = 7;
		Half2 half = new Half2();
		List<Integer> list = new ArrayList<Integer>();
		List<String> list2 = new ArrayList<String>();
		half.firstStep(a, b, c, 0);
		list=half.intList;
		boolean falg = half.isHalf(list);
		if (falg)
			list2.add(a+"-"+b+"-"+c);
//		for (a = 1; a < 10; a++) {
//			for (b = 1; b < 10; b++) {
//				for (c = 1; c < 10; c++) {
//					if (half.check(a, b, c)) {
//						half.firstStep(a, b, c, 0);
//						list=half.intList;
//						boolean falg = half.isHalf(list);
//						if (falg)
//							list2.add(a+"-"+b+"-"+c);
//					}
//				}
//			}
//		}
		
		for (String string : list2) {
			System.out.println(string);
		}
		
	}
}
