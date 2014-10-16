package nl.tno.timeseries.interfaces;

import java.io.Serializable;
import java.util.Map;

import nl.tno.storm.configuration.api.ZookeeperStormConfigurationAPI;

/**
 * An operation performs the processing of particles in a channel. This is the
 * abstract interface or the SingleOperation and the BatchOperation The
 * ChannelBolt manages the operations, each channel will have its own operation
 * instance. An operation is created at soon as the ChannelBolt gets a particle
 * with an unknown channelid.
 * 
 * An operation must also add the annotation OperationDeclaration annotation.
 * 
 * @author waaijbdvd
 * 
 */
public abstract interface Operation extends Serializable {

	/**
	 * Init this operation.
	 * 
	 * @param channelID
	 *            The id of the channel this operation is connected to. Mostly
	 *            the same channelid as the dataParticles that will be delivered
	 *            to this operation. In case of a ChannelGrouperBolt infront of
	 *            the ChannelBolt managing this operation, the channelGroupId
	 *            will be used.
	 * @param stormNativeConfig
	 *            A reference to the storm config object
	 * @param zookeeperStormConfiguration
	 *            A reference to the zookeeper storm config api
	 */
	public void init(String channelID,
			@SuppressWarnings("rawtypes") Map stormNativeConfig,
			ZookeeperStormConfigurationAPI zookeeperStormConfiguration)
			throws OperationException;

	/**
	 * @param startTimestamp
	 *            The timestamp of the first particle, this is the first
	 *            particle this operation instance have to process
	 * 
	 * @param startTimestamp
	 */
	public void prepareForFirstParticle(long startTimestamp)
			throws OperationException;

}
