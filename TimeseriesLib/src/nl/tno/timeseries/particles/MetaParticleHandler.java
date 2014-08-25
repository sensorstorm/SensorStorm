package nl.tno.timeseries.particles;

import nl.tno.timeseries.interfaces.MetaParticle;
import nl.tno.timeseries.interfaces.Operation;

/**
 * An object must implement this interface to become a handler for a specific
 * MetaParticle. Which particle must be defined using the
 * MetaParticleHandlerDeclaration annotation.
 * 
 * @author waaijbdvd
 * 
 */
public interface MetaParticleHandler {

	/**
	 * Initialize the metaParticle handler connected to the passed operation.
	 * 
	 * @param operation
	 * @param emitParticleHandler
	 */
	public void init(Operation operation,
			EmitParticleInterface emitParticleHandler);

	/**
	 * Passed when this specific metaParticle is received for the connected
	 * operation.
	 * 
	 * @param metaParticle
	 */
	public void handleMetaParticle(MetaParticle metaParticle);

}
