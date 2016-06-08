package com.kong.learning.bp;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Scanner;

public class Backpro {

	public static void main(String[] args) {
		String filename = new String("delta.in");
		try {
			FileInputStream fileInputStream = new FileInputStream(filename);
			Scanner scanner = new Scanner(fileInputStream);
			int attN, hidN, outN, samN;
			attN = scanner.nextInt();
			outN = scanner.nextInt();
			hidN = scanner.nextInt();
			samN = scanner.nextInt();
			double samin[][] = new double[samN][attN];
			double samout[][] = new double[samN][outN];
			for (int i = 0; i < samN; ++i) {
				for (int j = 0; j < attN; ++j) {
					samin[i][j] = scanner.nextDouble();
				}
				for (int j = 0; j < outN; ++j) {
					samout[i][j] = scanner.nextDouble();
				}
			}
			int times = 10000;
			double rate = 0.5;
			BP2 bp2 = new BP2(attN, outN, hidN, samN, times, rate);
			bp2.train(samin, samout);
			for (int i = 0; i < hidN; ++i) {
				for (int j = 0; j < attN; ++j)
					System.out.println(bp2.dw1[i][j] + "");
				System.out.println();
			}
			for (int i = 0; i < outN; ++i) {
				for (int j = 0; j < hidN; ++j)
					System.out.println(bp2.dw2[i][j] + "");
				System.out.println();
			}
			while (true) {
				double testout[] = new double[outN];
				double testin[] = new double[attN];
				Scanner testinScanner = new Scanner(System.in);
				for (int i = 0; i < attN; i++) {
					testin[i] = testinScanner.nextDouble();
				}
				testout = bp2.getResult(testin);
				for (int i = 0; i < outN; ++i)
					System.out.println(testout[i] + "");
				System.out.println();
			}
		} catch (FileNotFoundException e) {

			e.printStackTrace();
		}
		System.out.println("End");
	}
}
