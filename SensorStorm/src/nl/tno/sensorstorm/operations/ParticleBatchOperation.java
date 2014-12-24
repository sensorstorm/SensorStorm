package nl.tno.sensorstorm.operations;

import java.util.List;

import nl.tno.sensorstorm.particles.DataParticle;
import nl.tno.sensorstorm.particles.DataParticleBatch;

/**
 * A ParticleBatchOperation performs the processing of particles in a channel,
 * particles will be grouped by a Batcher into a list. The ChannelBolt manages
 * the operations, each channel will have its own operation instance. An
 * operation is created at soon as the ChannelBolt gets a particle with an
 * unknown channelid.
 * 
 * @author waaijbdvd
 */
public interface ParticleBatchOperation extends Operation {

	/**
	 * A new particle batch has arrived for this operation. A list containing
	 * zero or more particles can be returned to be sent further up into the
	 * topology.
	 * 
	 * @param inputParticleBatch
	 *            A batch of one or more data particle to be processed
	 * @return Returns a list of zero or more data particles to be sent further
	 *         up to the topology.
	 */
	public List<DataParticle> execute(DataParticleBatch inputParticleBatch)
			throws OperationException;

}
