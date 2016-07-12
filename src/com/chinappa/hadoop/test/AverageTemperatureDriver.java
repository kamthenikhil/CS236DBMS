package com.chinappa.hadoop.test;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.chinappa.hadoop.combiner.WeatherDataCombiner;
import com.chinappa.hadoop.dto.StationInformationDTO;
import com.chinappa.hadoop.dto.WeatherInformationDTO;
import com.chinappa.hadoop.mapper.StationInfomationMapper;
import com.chinappa.hadoop.mapper.WeatherDataMapper;
import com.chinappa.hadoop.reducer.StationInfomationReducer;
import com.chinappa.hadoop.reducer.WeatherDataReducer;

/**
 * This is the driver class to initiate the map-reduce jobs.
 * 
 * @author nikhil
 *
 */
public class AverageTemperatureDriver implements Tool {

	@Override
	public int run(String[] args) throws Exception {

		int errorCode = 1;

		Configuration conf = getConf();
		Job job1 = Job.getInstance(conf, "job1");

		job1.setJarByClass(AverageTemperatureDriver.class);

		job1.setMapperClass(StationInfomationMapper.class);
		job1.setReducerClass(StationInfomationReducer.class);
		job1.setNumReduceTasks(1);

		job1.setMapOutputKeyClass(IntWritable.class);
		job1.setMapOutputValueClass(StationInformationDTO.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[2]));

		errorCode = job1.waitForCompletion(true) ? 0 : 1;

		if (errorCode == 0) {
			/*
			 * Job 2
			 */
			Job job2 = Job.getInstance(conf, "job2");

			job2.setJarByClass(AverageTemperatureDriver.class);

			job2.addCacheFile(new URI(args[2]));

			job2.setMapperClass(WeatherDataMapper.class);
			job2.setCombinerClass(WeatherDataCombiner.class);
			job2.setReducerClass(WeatherDataReducer.class);
			job2.setNumReduceTasks(1);

			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(WeatherInformationDTO.class);

			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);

			job2.setInputFormatClass(TextInputFormat.class);
			job2.setOutputFormatClass(TextOutputFormat.class);

			FileInputFormat.addInputPath(job2, new Path(args[1]));
			FileOutputFormat.setOutputPath(job2, new Path(args[3]));

			errorCode = job2.waitForCompletion(true) ? 0 : 1;

			FileSystem fs = FileSystem.get(conf);
			fs.delete(new Path(args[2]), true);
		}
		return errorCode;
	}

	/**
	 * Method Name: main Return type: none Purpose:Read the arguments from
	 * command line and run the Job till completion
	 * 
	 */
	public static void main(String[] args) throws Exception {
		if (args.length != 4) {
			System.err
					.println("Enter valid number of arguments <StationDataDirectory> <WeatherDataDirectory> <TempOutputDirectory> <OutputLocation>");
			System.exit(0);
		}
		long startTime = System.currentTimeMillis();
		ToolRunner.run(new Configuration(), new AverageTemperatureDriver(),
				args);
		long stopTime = System.currentTimeMillis();
		long elapsedTime = stopTime - startTime;
		System.out.println("Time elapsed: " + elapsedTime / 1000 + " secs");
	}

	@Override
	public Configuration getConf() {
		return new Configuration();
	}

	@Override
	public void setConf(Configuration arg0) {
	}
}