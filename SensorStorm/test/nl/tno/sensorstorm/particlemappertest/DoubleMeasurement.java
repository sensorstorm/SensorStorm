package nl.tno.sensorstorm.particlemappertest;

import nl.tno.sensorstorm.api.annotation.TupleField;
import nl.tno.sensorstorm.api.particles.AbstractMetaParticle;
import nl.tno.sensorstorm.api.particles.MetaParticle;
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

	@Override
	public boolean equalMetaParticle(MetaParticle obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		DoubleMeasurement other = (DoubleMeasurement) obj;
		if (timestamp != other.timestamp) {
			return false;
		}
		return true;
	}

}
