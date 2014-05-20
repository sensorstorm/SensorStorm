package nl.tno.timeseries.test;

import nl.tno.timeseries.mapper.annotation.TupleField;
import nl.tno.timeseries.mapper.api.AbstractParticle;
import nl.tno.timeseries.mapper.api.Particle;

public class DoubleMeasurement extends AbstractParticle implements Particle {
	
	public DoubleMeasurement() {
	}
	
	public DoubleMeasurement(String streamId, long sequenceNr, double value) {
		this.streamId = streamId;
		this.sequenceNr = sequenceNr;
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
		return this.sequenceNr;
	}
	
}
