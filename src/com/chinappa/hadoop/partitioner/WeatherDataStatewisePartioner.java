//package com.chinappa.hadoop.partitioner;
//
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Partitioner;
//
//public class WeatherDataStatewisePartioner extends
//		Partitioner<WeatherDataTuple, Text> {
//	@Override
//	public int getPartition(WeatherDataTuple weatherDataTuple, Text text,
//			int numPartitions) {
//		return weatherDataTuple.getStateCode().hashCode() % numPartitions;
//	}
//}
