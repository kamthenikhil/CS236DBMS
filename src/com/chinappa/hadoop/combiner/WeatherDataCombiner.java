package com.chinappa.hadoop.combiner;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.chinappa.hadoop.constant.CommonConstant;
import com.chinappa.hadoop.dto.WeatherInformationDTO;

/**
 * The Combiner gets <State Code>#<Month Index> as key and list of <Temperature>
 * for that particular month as its input (at each node). It then calculates the
 * cumulative temperature for that month. But as combiner runs on each of the
 * mapper nodes, it also stores the number of entries corresponding to a month.
 * (This value will be used to calculate the average temperature at the reducer)
 * It then transmits the <State Code> as key and <Month Index>tab<Cumulative
 * Temperature>tab<Number of Entries> as values.
 * 
 * @author nikhil
 *
 */
public class WeatherDataCombiner extends
		Reducer<Text, WeatherInformationDTO, Text, WeatherInformationDTO> {

	public void reduce(Text key, Iterable<WeatherInformationDTO> values,
			Context context) throws IOException, InterruptedException {

		WeatherInformationDTO outputWeatherInformationDTO = new WeatherInformationDTO();
		float cumulativeTemp = 0.0f;
		float cumulativePrecipitation = 0.0f;
		Iterator<WeatherInformationDTO> valueIterator = values.iterator();
		int counter = 0;
		while (valueIterator.hasNext()) {
			WeatherInformationDTO inputWeatherInformationDTO = valueIterator
					.next();
			cumulativeTemp += inputWeatherInformationDTO.getTemperature();
			cumulativePrecipitation += inputWeatherInformationDTO
					.getPrecipitation();
			counter++;
		}
		String[] tokens = key.toString().split(CommonConstant.HASH);
		outputWeatherInformationDTO.setStateCode(tokens[0]);
		outputWeatherInformationDTO.setMonthIndex(Integer.parseInt(tokens[1]));
		outputWeatherInformationDTO.setCumulativeTemperature(cumulativeTemp);
		outputWeatherInformationDTO
				.setCumulativePrecipitation(cumulativePrecipitation);
		outputWeatherInformationDTO.setNumberOfEntries(counter);
		context.write(new Text(outputWeatherInformationDTO.getStateCode()),
				outputWeatherInformationDTO);
	}
}