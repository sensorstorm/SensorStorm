package nl.tno.sensortimeseries.model;

public class MeasurementDouble extends Measurement {

	public MeasurementDouble(String sensorid, long timestamp, Double value) {
		super(sensorid, timestamp, value, Double.class);
	}
	
	public Double getValue() {
		return (Double)getValue();
	}

}
