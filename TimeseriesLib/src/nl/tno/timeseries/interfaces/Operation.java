package nl.tno.timeseries.interfaces;

import java.io.Serializable;
import java.util.Map;

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
	 *            The id of the channel this operation is connected to
	 * @param startTimestamp
	 *            The timestamp of the first particle, this is the first
	 *            particle this operation instance have to process
	 * @param stormConfig
	 *            A reference to the storm config object
	 */
	public void init(String channelID, long startTimestamp,
			@SuppressWarnings("rawtypes") Map stormConfig);

}
