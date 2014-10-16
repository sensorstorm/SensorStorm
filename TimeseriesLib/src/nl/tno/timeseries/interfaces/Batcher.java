package nl.tno.timeseries.interfaces;

import java.util.List;
import java.util.Map;

import nl.tno.storm.configuration.api.ZookeeperStormConfigurationAPI;
import nl.tno.timeseries.channels.ParticleCache;

/**
 * An interface describing the batch functionality: to collect one or more
 * particles in a batch based on a certain criteria. Particles must be added to
 * an inner batch and maintained until the batch is ready to be sent.
 * 
 * @author waaijbdvd
 */
public interface Batcher {

	/**
	 * 
	 * @param channelID
	 * @param cache
	 *            The cache containing all {@link DataParticle} objects present
	 *            within the bolt
	 * @param stormNativeConfig
	 * @param zookeeperStormConfiguration
	 * @throws BatcherException
	 */
	public void init(String channelID, ParticleCache cache,
			@SuppressWarnings("rawtypes") Map stormNativeConfig,
			ZookeeperStormConfigurationAPI zookeeperStormConfiguration)
			throws BatcherException;

	public void prepareForFirstParticle(long startTimestamp)
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
	 */
	public List<DataParticleBatch> batch(DataParticle inputParticle)
			throws BatcherException;

}
