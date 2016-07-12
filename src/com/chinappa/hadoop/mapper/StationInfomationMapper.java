package com.chinappa.hadoop.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.chinappa.hadoop.constant.CommonConstant;
import com.chinappa.hadoop.dto.StationInformationDTO;

/**
 * This class is part of the map-reduce job to find the state code for the
 * entries where state code is missing. The Mapper gets the station data and
 * emits 'stations with state code' and 'stations without one' with different
 * integer keys. The value simply contains the station information.
 * 
 * @author nikhil
 *
 */
public class StationInfomationMapper extends
		Mapper<Object, Text, IntWritable, StationInformationDTO> {

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		if (line != null && !line.trim().startsWith(CommonConstant.USAF_TOKEN)) {
			line = line.replaceAll("^\"", "");
			line = line.replaceAll("\"$", "");
			if (!line.startsWith(CommonConstant.USAF_TOKEN)) {
				String[] tokens = line.split(CommonConstant.DOUBLE_QUOTE
						+ CommonConstant.COMMA + CommonConstant.DOUBLE_QUOTE);
				boolean isCountryCodeUS = true;
				if (tokens.length < 4
						|| !tokens[3].trim()
								.equalsIgnoreCase(CommonConstant.US)) {
					isCountryCodeUS = false;
				}
				if (isCountryCodeUS) {
					IntWritable outputKey = new IntWritable();
					String stationId = tokens[0];
					if (tokens.length >= 5) {
						String stateCode = tokens[4];
						String longitude = null;
						if (tokens.length >= 7) {
							longitude = tokens[6];
						}
						StationInformationDTO stationInformationDTO = null;
						if (stateCode != null && !stateCode.isEmpty()) {
							outputKey.set(CommonConstant.ONE);
							stationInformationDTO = new StationInformationDTO();
							stationInformationDTO.setStationId(stationId);
							stationInformationDTO.setState(stateCode);
							if (longitude != null && !longitude.isEmpty()) {
								Double longi = Double.parseDouble(longitude) % 180;
								if (longi > 0) {
									longi = -180 - (180 - longi);
								}
								stationInformationDTO.setLongitude(longi);
							}
						} else {
							if (longitude != null && !longitude.isEmpty()) {
								outputKey.set(CommonConstant.TWO);
								stationInformationDTO = new StationInformationDTO();
								stationInformationDTO.setStationId(stationId);
								stationInformationDTO
										.setState(CommonConstant.DUMMY_STATE_CODE);
								Double longi = Double.parseDouble(longitude) % 180;
								if (longi > 0) {
									longi = -180 - (180 - longi);
								}
								stationInformationDTO.setLongitude(longi);
							}

						}
						if (stationInformationDTO != null) {
							context.write(outputKey, stationInformationDTO);
						}
					}
				}
			}
		}
	}
}
