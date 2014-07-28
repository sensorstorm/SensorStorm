package nl.tno.timeseries.interfaces;

public interface MetaParticleHandler {

	public void init(Operation operation, EmitParticleInterface emitParticleHandler);

	public void handleMetaParticle(MetaParticle metaParticle);

}
