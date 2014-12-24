package nl.tno.sensorstorm.api.processing;

import java.util.List;

import nl.tno.sensorstorm.api.annotation.MetaParticleHandlerDeclaration;
import nl.tno.sensorstorm.api.particles.MetaParticle;
import nl.tno.sensorstorm.api.particles.Particle;

/**
 * An object must implement this interface to become a handler for a specific
 * type of {@link MetaParticle}. Which particle must be defined using the
 * {@link MetaParticleHandlerDeclaration} annotation.
 */
public interface MetaParticleHandler {

	/**
	 * Initialize the metaParticle handler connected to the passed operation.
	 * 
	 * @param operation
	 *            {@link Operation} this {@link MetaParticleHandler} must
	 *            interact with.
	 */
	void init(Operation operation);

	/**
	 * Passed when this specific metaParticle is received for the connected
	 * operation. The MetaParticle itself will automatically be passed up
	 * through the topology and does not have to be returned.
	 * 
	 * @param metaParticle
	 *            {@link MetaParticle} to process
	 * @return Returns a list containing new MetaParticles or DataParticles to
	 *         be submitted in the topology
	 */
	List<? extends Particle> handleMetaParticle(MetaParticle metaParticle);

}
