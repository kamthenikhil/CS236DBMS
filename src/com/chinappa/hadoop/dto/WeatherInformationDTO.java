package com.chinappa.hadoop.dto;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * This class stores the weather record.
 * @author nikhil
 *
 */
public class WeatherInformationDTO implements Writable {

	private String stateCode;

	private int monthIndex;

	private float temperature;

	private float precipitation;

	private float cumulativeTemperature;

	private float cumulativePrecipitation;

	private int numberOfEntries;

	public String getStateCode() {
		return stateCode;
	}

	public void setStateCode(String stateCode) {
		this.stateCode = stateCode;

	}

	public int getMonthIndex() {
		return monthIndex;
	}

	public void setMonthIndex(int monthIndex) {
		this.monthIndex = monthIndex;
	}

	public float getTemperature() {
		return temperature;
	}

	public void setTemperature(float temperature) {
		this.temperature = temperature;
	}

	public float getPrecipitation() {
		return precipitation;
	}

	public void setPrecipitation(float precipitation) {
		this.precipitation = precipitation;
	}

	public float getCumulativeTemperature() {
		return cumulativeTemperature;
	}

	public void setCumulativeTemperature(float cumulativeTemperature) {
		this.cumulativeTemperature = cumulativeTemperature;
	}

	public float getCumulativePrecipitation() {
		return cumulativePrecipitation;
	}

	public void setCumulativePrecipitation(float cumulativePrecipitation) {
		this.cumulativePrecipitation = cumulativePrecipitation;
	}

	public int getNumberOfEntries() {
		return numberOfEntries;
	}

	public void setNumberOfEntries(int numberOfEntries) {
		this.numberOfEntries = numberOfEntries;
	}

	@Override
	public void readFields(DataInput input) throws IOException {

		stateCode = input.readUTF();
		monthIndex = input.readInt();
		temperature = input.readFloat();
		precipitation = input.readFloat();
		cumulativeTemperature = input.readFloat();
		cumulativePrecipitation = input.readFloat();
		numberOfEntries = input.readInt();
	}

	@Override
	public void write(DataOutput output) throws IOException {

		output.writeUTF(stateCode);
		output.writeInt(monthIndex);
		output.writeFloat(temperature);
		output.writeFloat(precipitation);
		output.writeFloat(cumulativeTemperature);
		output.writeFloat(cumulativePrecipitation);
		output.writeInt(numberOfEntries);
	}
}
