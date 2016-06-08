package com.kong.learning.ant;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
/**
 * 主程序 调用ACO求解问题
 * 
 * @author FashionXu
 */
public class Main {
	/**
	 * @param args
	 *            the command line arguments
	 */
	public static void main(String[] args) {
		// TODO code application logic here
		Aco aco;
		aco = new Aco();
		try {
			aco.init("e://eil51.tsp", 50);
			aco.run(2000);
			aco.ReportResult();
		} catch (FileNotFoundException ex) {
			Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
		} catch (IOException ex) {
			Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
		}
	}
}