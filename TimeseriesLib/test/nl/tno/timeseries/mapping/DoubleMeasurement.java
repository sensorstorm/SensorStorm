package nl.tno.timeseries.mapping;

import nl.tno.timeseries.interfaces.Particle;
import nl.tno.timeseries.mapper.annotation.TupleField;
import nl.tno.timeseries.particles.AbstractParticle;

public class DoubleMeasurement extends AbstractParticle implements Particle {

	public DoubleMeasurement() {
	}

	public DoubleMeasurement(String streamId, long sequenceNr, double value) {
		this.channelId = streamId;
		this.timestamp = sequenceNr;
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

	public long getTimestamp() {
		return this.timestamp;
	}

}
