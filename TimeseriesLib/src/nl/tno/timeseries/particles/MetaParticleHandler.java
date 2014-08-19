package nl.tno.timeseries.particles;

import nl.tno.timeseries.interfaces.MetaParticle;
import nl.tno.timeseries.interfaces.Operation;

public interface MetaParticleHandler {

	public void init(Operation operation, EmitParticleInterface emitParticleHandler);

	public void handleMetaParticle(MetaParticle metaParticle);

}
