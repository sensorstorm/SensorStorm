package nl.tno.timeseries.interfaces;

import java.util.List;

/**
 * A SingleOperation performs the processing of particles in a channel, each
 * particle will be presented to this operation one by one. The ChannelBolt
 * manages the operations, each channel will have its own operation instance. An
 * operation is created at soon as the ChannelBolt gets a particle with an
 * unknown channelid.
 * 
 * @author waaijbdvd
 */
public interface SingleOperation extends Operation {

	/**
	 * A new particle has arrived for this operation. A list containing zero or
	 * more particles can be returned to be sent further up into the topology.
	 * 
	 * @param inputParticle
	 *            The dataParticle that need to be processed.
	 * @return Returns a list of zero or more data particles to be sent further
	 *         up to the topology.
	 */
	public List<DataParticle> execute(DataParticle inputParticle);

}
