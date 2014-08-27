package nl.tno.timeseries.particles;

import java.util.List;

import nl.tno.timeseries.interfaces.MetaParticle;
import nl.tno.timeseries.interfaces.Operation;
import nl.tno.timeseries.interfaces.Particle;

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
	 */
	public void init(Operation operation);

	/**
	 * Passed when this specific metaParticle is received for the connected
	 * operation. The MetaParticle itself will automatically be passed up
	 * through the topology and does not have to be returned.
	 * 
	 * @param metaParticle
	 * @return Returns a list containing MetaParticles or DataParticles
	 */
	public List<Particle> handleMetaParticle(MetaParticle metaParticle);

}
