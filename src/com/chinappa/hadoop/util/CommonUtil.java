package com.chinappa.hadoop.util;

import com.chinappa.hadoop.constant.CommonConstant;

/**
 * This class handles all the common tasks.
 * 
 * @author nikhil
 */
public class CommonUtil {

	public static String getNameOfMonthFromIndex(int minTempMonNumber) {

		String monthName = null;
		switch (minTempMonNumber) {
		case 1:
			monthName = CommonConstant.JANUARY;
			break;
		case 2:
			monthName = CommonConstant.FEBRUARY;
			break;
		case 3:
			monthName = CommonConstant.MARCH;
			break;
		case 4:
			monthName = CommonConstant.APRIL;
			break;
		case 5:
			monthName = CommonConstant.MAY;
			break;
		case 6:
			monthName = CommonConstant.JUNE;
			break;
		case 7:
			monthName = CommonConstant.JULY;
			break;
		case 8:
			monthName = CommonConstant.AUGUST;
			break;
		case 9:
			monthName = CommonConstant.SEPTEMBER;
			break;
		case 10:
			monthName = CommonConstant.OCTOBER;
			break;
		case 11:
			monthName = CommonConstant.NOVEMBER;
			break;
		case 12:
			monthName = CommonConstant.DECEMBER;
			break;
		default:
			break;
		}
		return monthName;
	}
}
