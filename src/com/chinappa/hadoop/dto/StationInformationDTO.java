package com.chinappa.hadoop.dto;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * The following class stored the station information.
 * @author nikhil
 *
 */
public class StationInformationDTO implements Writable {

	private String stationId;

	private String state;

	private double longitude;

	@Override
	public void readFields(DataInput input) throws IOException {

		stationId = input.readUTF();
		state = input.readUTF();
		longitude = input.readDouble();
	}

	@Override
	public void write(DataOutput output) throws IOException {

		output.writeUTF(stationId);
		output.writeUTF(state);
		output.writeDouble(longitude);
	}

	public String getStationId() {
		return stationId;
	}

	public void setStationId(String stationId) {
		this.stationId = stationId;
	}

	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}

	public double getLongitude() {
		return longitude;
	}

	public void setLongitude(double longitude) {
		this.longitude = longitude;
	}
}
