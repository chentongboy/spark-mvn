package com.kong.learning.full;

import java.util.ArrayList;
import java.util.List;

/**
 * 两满容器通过另一较大空容器实现总量分半
 * 
 * @author kong
 * 
 */
public class Half {

	int amax;
	int bmax;
	int cmax;

	public Half() {
	}

	List<Integer> aList = new ArrayList<Integer>();
	List<Integer> bList = new ArrayList<Integer>();
	List<Integer> cList = new ArrayList<Integer>();
	List<Integer> dList = new ArrayList<Integer>();

	/**
	 * 是否可分
	 * 
	 * @param a
	 *            大容量
	 * @param b
	 *            中容量
	 * @param c
	 *            小容量
	 * @return true 成功 false 失败
	 */
	public boolean isHalf(int a, int b, int c) {
		int flag = 0;
		boolean bln = true;

		amax = a;
		bmax = b;
		cmax = c;
		int d = 0;

		if (a != 0) {
			a = 0;
		}

		List<Integer> firstStep = firstStep(a, b, c);
		a = firstStep.get(0);
		b = firstStep.get(1);
		c = firstStep.get(2);

		while (bln) {
			Integer finalStep = finalStep(a, b, c);

			if (finalStep == 2) {
				flag = 2;
				bln = false;
			} else if (finalStep == 3) {
				flag = 3;
				bln = false;
			} else {
				a = aList.get(aList.size() - 1);
				b = bList.get(bList.size() - 1);
				c = cList.get(cList.size() - 1);

			}
			d++;
			if (d == 100) {
				flag = 3;
				bln = false;
			}
		}

		if (flag == 3) {

			return false;
		} else if (flag == 2) {

			dList.add(d);
			return true;
		} else {

			return false;
		}
	}

	/**
	 * 处理的第一步
	 * 
	 * @param a
	 * @param b
	 * @param c
	 * @return 第一步处理结果
	 */
	public List<Integer> firstStep(int a, int b, int c) {
		List<Integer> intList = new ArrayList<Integer>();
		a = amax;
		b = 0;
		int d = amax - bmax;
		if (d > c) {
			c = 0;
		} else {
			b = c - d;
			c = 0;
		}
		intList.add(a);
		intList.add(b);
		intList.add(c);
		return intList;
	}

	/**
	 * 后续步骤
	 * 
	 * @param a
	 * @param b
	 * @param c
	 * @return 1.可继续分 2.分半成功 3.不可分
	 */
	public Integer finalStep(int a, int b, int c) {

		boolean bl = true;
		if (a == amax) {
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
				if (b == bmax) {
					bl = false;
				}
				aList.add(a);
				bList.add(b);
				cList.add(c);
				if (a == ((bmax + cmax) / 2)) {
					return 2;
				} else if (a < 0) {
					return 3;
				}

			}
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
				aList.add(a);
				bList.add(b);
				cList.add(c);

				if (a == ((bmax + cmax) / 2)) {
					return 2;
				} else if (a < 0) {
					return 3;
				}
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
					aList.add(a);
					bList.add(b);
					cList.add(c);

				} else {
					a = a + b;
					b = c;
					c = 0;
					aList.add(a);
					bList.add(b);
					cList.add(c);

					if (a == ((bmax + cmax) / 2)) {
						return 2;
					} else if (a < 0) {
						return 3;
					}
				}
			}
		}

		return 1;
	}

	/**
	 * 分半步数
	 * 
	 * @return
	 */
	public List<Integer> bushu() {
		return dList;
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
		int a = 1, b = 1, c = 1;
		Half half = new Half();
		List<Object> list = new ArrayList<Object>();
		List<Integer> list3 = new ArrayList<Integer>();
		for (a = 1; a < 200; a++) {
			for (b = 1; b < 100; b++) {
				for (c = 1; c < 100; c++) {
					if (half.check(a, b, c)) {
						boolean name = half.isHalf(a, b, c);
						if (name) {
							List<Integer> list2 = new ArrayList<Integer>();
							list2.add(a);
							list2.add(b);
							list2.add(c);
							list.add(list2);
							list3.add(list2.get(1) + list2.get(2) - list2.get(0));
						}
					}
				}
			}
		}

		List<Integer> bushu = half.bushu();

		for (int i = 0; i < list.size(); i++) {
			if (list3.get(i) > 20 && bushu.get(i) > 10) {
				System.out.println(list.get(i) + " 步数" + bushu.get(i) + " 容量差" + list3.get(i));
			}
		}
	}

}
