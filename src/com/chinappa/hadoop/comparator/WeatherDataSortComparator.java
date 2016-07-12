//package com.chinappa.hadoop.comparator;
//
//import org.apache.hadoop.io.WritableComparable;
//import org.apache.hadoop.io.WritableComparator;
//
///**
// * This class is used to sort the records (reducer input) on the basis of some
// * attribute in the composite key.
// * 
// * @author nikhil
// *
// */
//public class WeatherDataSortComparator extends WritableComparator {
//
//	public WeatherDataSortComparator() {
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
//		return tuple1.getTemperature().compareTo(tuple2.getTemperature());
//	}
//}
