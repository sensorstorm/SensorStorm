package nl.tno.sensorstorm.particlemappertest;

import nl.tno.sensorstorm.api.annotation.TupleField;
import nl.tno.sensorstorm.api.particles.AbstractMetaParticle;
import nl.tno.sensorstorm.api.particles.Particle;

public class DoubleMeasurement extends AbstractMetaParticle implements Particle {

	public DoubleMeasurement() {
	}

	public DoubleMeasurement(String sensorId, long timestamp, double value) {
		super(timestamp);
		this.sensorId = sensorId;
		this.value = value;
	}

	@TupleField
	private double value;

	public double getValue() {
		return value;
	}

	public void setValue(double value) {
		this.value = value;
	}

	@TupleField
	private String sensorId;

	public String getSensorId() {
		return sensorId;
	}

	public void setSensorId(String sensorId) {
		this.sensorId = sensorId;
	}

}
