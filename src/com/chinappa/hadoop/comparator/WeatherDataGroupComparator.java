//package com.chinappa.hadoop.comparator;
//
//import org.apache.hadoop.io.WritableComparable;
//import org.apache.hadoop.io.WritableComparator;
//
///**
// * This class is used to group the records (reducer input) on the basis of the
// * primary key.
// * 
// * @author nikhil
// *
// */
//public class WeatherDataGroupComparator extends WritableComparator {
//
//	public WeatherDataGroupComparator() {
//		super(WeatherDataTuple.class, true);
//	}
//
//	@SuppressWarnings("rawtypes")
//	@Override
//	public int compare(WritableComparable firstTuple,
//			WritableComparable secondTuple) {
//
//		WeatherDataTuple tuple1 = (WeatherDataTuple) firstTuple;
//		WeatherDataTuple tuple2 = (WeatherDataTuple) secondTuple;
//
//		/**
//		 * Group according to the state code.
//		 */
//		return tuple1.getStateCode().compareTo(tuple2.getStateCode());
//	}
//}
