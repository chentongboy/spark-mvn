package com.kong.learning.bp;

public class BP2 {

	public double[][] dw1;
	public double[][] dw2;
	/**
	 * 隐藏层单元个数
	 */
	int hidN;
	/**
	 * 学习样例个数
	 */
	int samN;
	/**
	 * 输入单元个数
	 */
	int attN;
	/**
	 * 输出单元个数
	 */
	int outN;
	/**
	 * 迭代次数
	 */
	int times;
	/**
	 * 学习效率
	 */
	double rate;
	boolean trained = false;

	public BP2(int attN, int outN, int hidN, int samN, int times, double rate) {

		this.attN = attN;
		this.outN = outN;
		this.hidN = hidN;
		this.samN = samN;
		dw1 = new double[hidN][attN + 1];//每行最后的一个是阀值w0
		for (int i = 0; i < hidN; ++i) {//每行代表所有输入到i隐藏单元的权值
			for (int j = 0; j < attN; ++j)
				dw1[i][j] = Math.random() / 2;
		}
		dw2 = new double[outN][hidN + 1];//输出层权值，每行最后一个是阀值w0
		for (int i = 0; i < outN; ++i) {//每行代表所有隐藏单元到i输出单元的权值
			for (int j = 0; j < hidN; ++j)
				dw2[i][j] = Math.random() / 2;
		}
		this.times = times;
		this.rate = rate;
	}

	public void train(double[][] samin, double[][] samout) {

		double dis = 0; //总体误差
		int count = times;
		double temphid[] = new double[hidN];
		double tempout[] = new double[outN];
		double wcount[] = new double[outN];
		double wchid[] = new double[hidN];
		while ((count--) > 0) {//迭代训练
			dis = 0;
			for (int i = 0; i < samN; ++i) {//遍历每个样例samin[i]
				for (int j = 0; j < hidN; ++j) {//计算每个隐藏层单元的结果
					temphid[j] = 0;
					for (int k = 0; k < attN; ++k)
						temphid[j] += dw1[j][k] * samin[i][k];
					temphid[j] += dw1[j][attN];//计算阀值产生的隐藏层结果
					temphid[j] = 1.0 / (1 + Math.exp(-temphid[j]));
				}
				for (int j = 0; j < hidN; ++j) {//计算每个输出层单元的结果
					tempout[j] = 0;
					for (int k = 0; k < hidN; ++k)
						tempout[j] += dw2[j][k] * temphid[k];
					tempout[j] += dw2[j][hidN];//计算阀值产生的输出结果
					tempout[j] = 1.0 / (1 + Math.exp(-tempout[j]));
				}
				//计算每个输出单元的误差项
				for (int j = 0; j < outN; ++j) {
					wcount[j] = tempout[j] * (1 - tempout[j]) * (samout[i][j] - tempout[j]);
					dis += Math.pow(samout[i][j] - tempout[j], 2);
				}
				//计算每个隐藏单元的误差项
				for (int j = 0; j < hidN; ++j) {
					double wche = 0;
					for (int k = 0; k < outN; ++k)//计算输出项的误差和
						wche += wcount[k] * dw2[k][j];
					wchid[j] = temphid[j] * (1 - temphid[j]) * wche;
				}
				//改变输出层的权值
				for (int j = 0; j < outN; ++j) {
					for (int k = 0; k < hidN; ++k)
						dw2[j][k] += rate * wcount[j] * temphid[k];
					dw2[j][hidN] = rate * wcount[j];
				}
				//改变隐藏层的权值
				for (int j = 0; j < hidN; ++j) {
					for (int k = 0; k < wchid.length; ++k)
						dw1[j][k] += rate * wchid[j] * samin[i][k];
					dw1[j][attN] = rate * wchid[j];
				}

			}
			if (dis < 0.003)
				break;
		}
		trained = true;
	}

	public double[] getResult(double[] samin) {

		double temphid[] = new double[hidN];
		double tempout[] = new double[outN];
		if (trained == false)
			return null;
		//计算每个隐藏层单元的结果
		for (int i = 0; i < hidN; ++i) {
			temphid[i] = 0;
			for (int j = 0; j < attN; ++j)
				temphid[j] += dw1[i][j] * samin[j];
			temphid[i] += dw1[i][attN];//计算阀值产生的隐藏层结果
			temphid[i] = 1.0 / (1 + Math.exp(-temphid[i]));
		}
		//计算每个输出层单元的结果
		for (int i = 0; i < outN; ++i) {
			tempout[i] = 0;
			for (int j = 0; j < hidN; ++j)
				tempout[j] += dw1[i][j] * temphid[j];
			tempout[i] += dw2[i][hidN];//计算阀值产生的输出结果
			tempout[i] = 1.0 / (1 + Math.exp(-tempout[i]));
		}
		return tempout;
	}

}
