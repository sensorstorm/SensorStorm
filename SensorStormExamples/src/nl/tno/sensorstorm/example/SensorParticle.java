package nl.tno.sensorstorm.example;

import nl.tno.sensorstorm.mapper.annotation.TupleField;
import nl.tno.sensorstorm.particles.AbstractDataParticle;

public class SensorParticle extends AbstractDataParticle {

	public SensorParticle() {

	}

	public SensorParticle(long timestamp, String sensorId, double measurement) {
		setTimestamp(timestamp);
		this.sensorId = sensorId;
		this.measurement = measurement;
	}

	@TupleField
	private String sensorId;
	@TupleField
	private double measurement;

	public String getSensorId() {
		return sensorId;
	}

	public void setSensorId(String sensorId) {
		this.sensorId = sensorId;
	}

	public double getMeasurement() {
		return measurement;
	}

	public void setMeasurement(double mesaurement) {
		measurement = mesaurement;
	}

	@Override
	public String toString() {
		return "SensorParticle [sensorId=" + sensorId + ", mesaurement="
				+ measurement + ", timestamp=" + timestamp + "]";
	}

}
