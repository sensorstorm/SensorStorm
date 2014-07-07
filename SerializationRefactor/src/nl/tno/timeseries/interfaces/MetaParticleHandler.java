package nl.tno.timeseries.interfaces;

import java.util.List;

public interface MetaParticleHandler {
	
	public void init(Operation operation);

	public List<Particle> execute(MetaParticle metaParticle);

}
