package com.chinappa.hadoop.reducer;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.chinappa.hadoop.constant.CommonConstant;
import com.chinappa.hadoop.dto.WeatherInformationDTO;
import com.chinappa.hadoop.util.CommonUtil;

/**
 * Reducer will get <State Code> as key and list of <Month Index>tab<Cumulative
 * Temperature>tab<Number of Entries> as values from all the combiners. It then
 * computes the average temperature for each month using above data and stores
 * in an array. It then calculates the month with the highest average
 * temperature and month with the lowest average temperature and the difference
 * between the two values per state. The values computed are stored in a local
 * TreeMap, where difference between the temperature is the key and information
 * corresponding to the record as value (TreeMap stores keys in a sorted
 * manner). While cleaning up the reducer writes the required output in
 * ascending order of the difference in temperatures in HDFS.
 * 
 * @author nikhil
 *
 */
public class WeatherDataReducer extends
		Reducer<Text, WeatherInformationDTO, Text, Text> {

	TreeMap<Float, String> sortedWeatherData = new TreeMap<Float, String>();

	public void reduce(Text key, Iterable<WeatherInformationDTO> values,
			Context context) throws IOException, InterruptedException {

		float[] tempPerMonth = new float[12];
		float[] precipitationPerMonth = new float[12];
		int[] numberOfEntries = new int[12];
		Iterator<WeatherInformationDTO> valuesIterator = values.iterator();
		int monthIndex;
		float interimCumulativeTemp;
		float interimCumulativePrecipitation;
		int interimNumberOfEntries;
		while (valuesIterator.hasNext()) {
			WeatherInformationDTO weatherInformationDTO = valuesIterator.next();
			monthIndex = weatherInformationDTO.getMonthIndex();
			interimCumulativeTemp = weatherInformationDTO
					.getCumulativeTemperature();
			interimCumulativePrecipitation = weatherInformationDTO
					.getCumulativePrecipitation();
			interimNumberOfEntries = weatherInformationDTO.getNumberOfEntries();
			tempPerMonth[monthIndex - 1] += interimCumulativeTemp;
			precipitationPerMonth[monthIndex - 1] += interimCumulativePrecipitation;
			numberOfEntries[monthIndex - 1] += interimNumberOfEntries;
		}

		for (int i = 0; i < 12; i++) {
			tempPerMonth[i] = tempPerMonth[i] / numberOfEntries[i];
			precipitationPerMonth[i] = precipitationPerMonth[i]
					/ numberOfEntries[i];
		}

		Float maxAveTemp = Float.MIN_VALUE;
		Float minAveTemp = Float.MAX_VALUE;
		int minTempMonthNumber = 0;
		int maxTempMonthNumber = 0;
		for (int i = 0; i < 12; i++) {
			float aveTemp = tempPerMonth[i];
			if (aveTemp > maxAveTemp) {
				maxAveTemp = aveTemp;
				maxTempMonthNumber = i + 1;
			}
			if (aveTemp < minAveTemp) {
				minAveTemp = aveTemp;
				minTempMonthNumber = i + 1;
			}
		}
		float diff = maxAveTemp - minAveTemp;
		String valueText = prepareOuteoutValueText(maxAveTemp,
				precipitationPerMonth[maxTempMonthNumber - 1], minAveTemp,
				precipitationPerMonth[minTempMonthNumber - 1],
				maxTempMonthNumber, minTempMonthNumber, diff);
		// context.write(key, new Text(valueText));
		sortedWeatherData.put(diff, key + CommonConstant.HASH + valueText);
	}

	/**
	 * The following method simply builds a output string in desired format.
	 * 
	 * @param maxAveTemp
	 * @param minAveTemp
	 * @param maxTempMonthNumber
	 * @param minTempMonthNumber
	 * @param diff
	 * @return
	 */
	private String prepareOuteoutValueText(float maxAveTemp,
			float precipitaionMaxMonth, float minAveTemp,
			float precipitaionMinMonth, int maxTempMonthNumber,
			int minTempMonthNumber, float diff) {
		DecimalFormat decimalFormat = new DecimalFormat(
				CommonConstant.DECIMAL_FORMAT);
		StringBuilder valueText = new StringBuilder();
		valueText.append(decimalFormat.format(maxAveTemp));
		valueText.append(CommonConstant.COMMA);
		valueText.append(decimalFormat.format(precipitaionMaxMonth));
		valueText.append(CommonConstant.COMMA);
		valueText
				.append(CommonUtil.getNameOfMonthFromIndex(maxTempMonthNumber));
		valueText.append(CommonConstant.TAB);
		valueText.append(decimalFormat.format(minAveTemp));
		valueText.append(CommonConstant.COMMA);
		valueText.append(decimalFormat.format(precipitaionMinMonth));
		valueText.append(CommonConstant.COMMA);
		valueText
				.append(CommonUtil.getNameOfMonthFromIndex(minTempMonthNumber));
		valueText.append(CommonConstant.TAB);
		valueText.append(decimalFormat.format(diff));
		return valueText.toString();
	}

	@Override
	public void cleanup(Context context) throws IOException,
			InterruptedException {
		for (Float diff : sortedWeatherData.keySet()) {
			String[] data = sortedWeatherData.get(diff).split(
					CommonConstant.HASH);
			context.write(new Text(data[0]), new Text(data[1]));
		}
	}
}
