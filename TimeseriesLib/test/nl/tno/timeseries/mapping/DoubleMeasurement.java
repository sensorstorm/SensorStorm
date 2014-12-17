package nl.tno.timeseries.mapping;

import nl.tno.sensorstorm.mapper.annotation.TupleField;
import nl.tno.sensorstorm.particles.AbstractMetaParticle;
import nl.tno.sensorstorm.particles.Particle;

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
