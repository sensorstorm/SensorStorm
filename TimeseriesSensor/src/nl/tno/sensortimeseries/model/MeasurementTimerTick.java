package nl.tno.sensortimeseries.model;

public class MeasurementTimerTick extends Measurement {

	public MeasurementTimerTick(String sensorid, long timestamp) {
		super(sensorid, timestamp, null, TimerTick.class);
	}

}
