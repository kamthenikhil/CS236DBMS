package com.chinappa.hadoop.configuration;

import org.apache.hadoop.mapred.JobConf;

/**
 * The following class is used for get configuration for different map-reduce
 * jobs.
 * 
 * @author nikhil
 *
 */
public class CommonConfiguration {

	public static JobConf fetchJobConfiguration() {

		JobConf conf = new JobConf();
//
//		conf.setJobName(CommonConstant.JOB_NAME);
//
//		conf.setOutputKeyClass(Text.class);
//		conf.setOutputValueClass(Text.class);
//
//		conf.setCombinerClass(WeatherDataCombiner.class);
//		conf.setReducerClass(WeatherDataReducer.class);
//
//		conf.setInputFormat(TextInputFormat.class);
//		conf.setOutputFormat(TextOutputFormat.class);
		return conf;
	}
}
