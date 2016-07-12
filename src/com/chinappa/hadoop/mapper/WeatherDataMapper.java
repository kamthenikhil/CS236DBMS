package com.chinappa.hadoop.mapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.chinappa.hadoop.constant.CommonConstant;
import com.chinappa.hadoop.dto.WeatherInformationDTO;

/**
 * This class is a art of map-reduce job to aggregate the weather data
 * pertaining to a state. The Mapper gets the weather data and finds the
 * corresponding state code from the local HashMap. (which is populated using
 * the local cache) It then emits the <State Code>#<Month Index> as key and
 * <Temperature> (corresponding to the month index) as its value.
 * 
 * @author nikhil
 *
 */
public class WeatherDataMapper extends
		Mapper<Object, Text, Text, WeatherInformationDTO> {

	private Map<String, WeatherInformationDTO> stationMap = new HashMap<String, WeatherInformationDTO>();

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		URI[] uriArray = context.getCacheFiles();
		for (URI uri : uriArray) {
			File folder = new File(uri.toString());
			File[] files = folder.listFiles();
			if (files != null && files.length > 0) {
				BufferedReader reader = null;
				for (File file : files) {
					if (file.getName().equals("part-r-00000")) {
						reader = new BufferedReader(new FileReader(file));
						String line = reader.readLine();
						while (line != null) {
							WeatherInformationDTO weatherInformationDTO = new WeatherInformationDTO();
							String[] tokens = line.split(CommonConstant.TAB);
							weatherInformationDTO.setStateCode(tokens[1]);
							stationMap.put(tokens[0], weatherInformationDTO);
							line = reader.readLine();
						}
						reader.close();
					}
				}
			}
		}
	}

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		if (line != null && !line.trim().startsWith(CommonConstant.STN_TOKEN)) {
			/**
			 * Extracting weather record info.
			 */
			String[] tokens = line.split(CommonConstant.SPACE_REGEX);
			String stationKey = tokens[0];
			int month = Integer.parseInt(tokens[2].substring(4, 6));
			String temperature = tokens[3];
			String precipitation = null;
			/**
			 * Fetching station info.
			 */
			WeatherInformationDTO weatherInformationDTO = stationMap
					.get(stationKey);
			if (weatherInformationDTO != null
					&& !weatherInformationDTO.getStateCode().isEmpty()) {
				if (tokens.length > 19) {
					precipitation = tokens[19].replaceAll("[A-Z]$", "");
					weatherInformationDTO.setPrecipitation(Float
							.parseFloat(precipitation));
				}
				if (!weatherInformationDTO.getStateCode().isEmpty()) {
					weatherInformationDTO.setMonthIndex(month);
					if (temperature != null && !temperature.isEmpty()) {
						weatherInformationDTO.setTemperature(Float
								.parseFloat(temperature));
					}
					if (precipitation != null && !precipitation.isEmpty()) {
						weatherInformationDTO.setPrecipitation(Float
								.parseFloat(precipitation));
					}
					context.write(
							new Text(weatherInformationDTO.getStateCode()
									+ CommonConstant.HASH
									+ weatherInformationDTO.getMonthIndex()),
							weatherInformationDTO);
				}
			}
		}
	}

}
