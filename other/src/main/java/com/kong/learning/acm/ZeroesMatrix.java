package com.kong.learning.acm;

public class ZeroesMatrix {

	public void setZeroes(int[][] matrix) {
		
		int m = matrix.length;
		if (m == 0)
			return;
		int n = matrix[0].length;
		if (n == 0)
			return;

		// find a zero element
		int zeroLabelRowIndex = -1;
		int zeroLabelColIndex = -1;
		boolean found = false;

		for (int i = 0; i < m; i++) {
			for (int j = 0; j < n; j++) {
				if (matrix[i][j] == 0) {
					zeroLabelRowIndex = i;
					zeroLabelColIndex = j;
					found = true;
					break;
				}
			}
			if (found)
				break;
		}
		if (!found)
			return;
		for (int i = 0; i < m; i++) {
			if (i == zeroLabelRowIndex)
				continue;
			for (int j = 0; j < n; j++) {
				if (j == zeroLabelColIndex)
					continue;
				if (matrix[i][j] == 0) {
					matrix[i][zeroLabelColIndex] = 0;
					matrix[zeroLabelRowIndex][j] = 0;
				}
			}
		}

		for (int i = 0; i < m; i++) {
			if (i == zeroLabelRowIndex)
				continue;
			if (matrix[i][zeroLabelColIndex] == 0) {
				for (int j = 0; j < n; j++) {
					matrix[i][j] = 0;
				}
			}
		}

		for (int j = 0; j < n; j++) {
			if (j == zeroLabelColIndex)
				continue;
			if (matrix[zeroLabelRowIndex][j] == 0) {
				for (int i = 0; i < m; i++) {
					matrix[i][j] = 0;
				}
			}
		}

		for (int i = 0; i < m; i++) {
			matrix[i][zeroLabelColIndex] = 0;
		}
		for (int j = 0; j < n; j++) {
			matrix[zeroLabelRowIndex][j] = 0;
		}

	}
}
