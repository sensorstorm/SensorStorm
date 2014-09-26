package nl.tno.timeseries.particles;

import nl.tno.timeseries.interfaces.Particle;
import backtype.storm.tuple.Tuple;

public interface EmitParticleInterface {

	public void emitParticle(Tuple anchor, Particle particle);

	public void emitParticle(Particle particle);

}
