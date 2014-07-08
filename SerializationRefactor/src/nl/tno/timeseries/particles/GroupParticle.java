package nl.tno.timeseries.particles;

import nl.tno.timeseries.interfaces.GroupedParticle;
import nl.tno.timeseries.interfaces.Particle;
import nl.tno.timeseries.mapper.annotation.TupleField;

public class GroupParticle extends AbstractParticle implements GroupedParticle {
	
	@TupleField
	private Particle particle;
	
	public GroupParticle() {
	}
	
	public GroupParticle(String channelId, long sequenceNr, Particle particle) {
		super(channelId, sequenceNr);
		this.particle = particle;
	}

	@Override
	public void setParticle(Particle particle) {
		this.particle = particle;
	}

	@Override
	public Particle getParticle() {
		return particle;
	}


}
