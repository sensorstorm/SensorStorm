package nl.tno.sensorstorm.api.processing;

import java.util.List;
import java.util.Map;

import nl.tno.sensorstorm.api.particles.DataParticle;
import nl.tno.sensorstorm.api.particles.DataParticleBatch;
import nl.tno.sensorstorm.api.particles.Particle;
import nl.tno.storm.configuration.api.ExternalStormConfiguration;

/**
 * An interface describing the batch functionality: to collect one or more
 * particles in a batch based on a certain criteria. Particles must be added to
 * an inner batch and maintained until the batch is ready to be sent.
 */
public interface Batcher {

	/**
	 * Initialize the Batcher.
	 * 
	 * @param fieldGrouper
	 *            When this bolt uses field grouping, the value of the field
	 *            which is used for grouping, null otherwise
	 * @param startTimestamp
	 *            Timestamp of the first {@link Particle}
	 * @param stormNativeConfig
	 *            Native Storm Configuration
	 * @param externalStormConfiguration
	 *            Reference to the {@link ExternalStormConfiguration}
	 * @throws BatcherException
	 *             When an error occurs in this {@link Batcher}
	 */
	void init(String fieldGrouper, long startTimestamp,
			@SuppressWarnings("rawtypes") Map stormNativeConfig,
			ExternalStormConfiguration externalStormConfiguration)
			throws BatcherException;

	/**
	 * A method to add a new data particle to the inner batch. If the batch(es)
	 * are ready to be processed, they can be returned as a list of one or more
	 * batches. If no batch is ready yet, null must be returned.
	 * 
	 * @param inputParticle
	 *            The new data particle to be added to the inner batch.
	 * @return Returns null to indicate the batch is not ready yet, or a list
	 *         with one or more DataPatricleBatches
	 * @throws BatcherException
	 *             When an error occurs in this {@link Batcher}
	 */
	List<DataParticleBatch> batch(DataParticle inputParticle)
			throws BatcherException;

}
