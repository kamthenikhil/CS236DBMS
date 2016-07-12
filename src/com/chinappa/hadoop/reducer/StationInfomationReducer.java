package com.chinappa.hadoop.reducer;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.chinappa.hadoop.constant.CommonConstant;
import com.chinappa.hadoop.dto.StationInformationDTO;

/**
 * This class is a part of map-reduce job to find the state code for the entries
 * where state code is missing. Using hadoop internal mechanism for sort and
 * shuffle, the reducer gets all the entries for these keys in a particular
 * order (List of stations with state ids are processed first and then the list
 * of stations with no predefined state ids.) The first list of entries is used
 * to compute the mean longitude for all the entries with state code and store
 * it locally. The second list of entries are then processed, where each entry
 * is checked against the mean longitude and partitioned according to its
 * location with respect to the mean longitude.
 * 
 * @author nikhil
 *
 */
public class StationInfomationReducer extends
		Reducer<IntWritable, StationInformationDTO, Text, Text> {

	private double cumulativeLongitude;

	private int numberOfReading;

	public void reduce(IntWritable key, Iterable<StationInformationDTO> values,
			Context context) throws IOException, InterruptedException {

		Iterator<StationInformationDTO> valuesIterator = values.iterator();
		StationInformationDTO stationInformationDTO = null;
		if (key.get() == CommonConstant.ONE) {
			while (valuesIterator.hasNext()) {
				stationInformationDTO = new StationInformationDTO();
				stationInformationDTO = valuesIterator.next();
				if (stationInformationDTO.getLongitude() != 0.0) {
					cumulativeLongitude += stationInformationDTO.getLongitude();
					numberOfReading += 1;
				}
				context.write(new Text(stationInformationDTO.getStationId()),
						new Text(stationInformationDTO.getState()));
			}
		} else if (key.get() == CommonConstant.TWO) {
			Double meanLongitude = cumulativeLongitude / numberOfReading;
			while (valuesIterator.hasNext()) {
				stationInformationDTO = new StationInformationDTO();
				stationInformationDTO = valuesIterator.next();
				Double longi = stationInformationDTO.getLongitude() % 180;
				if (longi > 0) {
					longi = -180 - (180 - longi);
				}
				if (longi > meanLongitude) {
					stationInformationDTO
							.setState(CommonConstant.ATLANTIC_OCEAN_STATE_CODE);
				} else {
					stationInformationDTO
							.setState(CommonConstant.PACIFIC_OCEAN_STATE_CODE);
				}
				context.write(new Text(stationInformationDTO.getStationId()),
						new Text(stationInformationDTO.getState()));
			}
		}
	}
}
