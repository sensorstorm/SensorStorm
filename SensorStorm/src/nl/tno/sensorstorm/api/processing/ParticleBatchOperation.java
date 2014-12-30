package nl.tno.sensorstorm.api.processing;

import java.util.List;

import nl.tno.sensorstorm.api.annotation.OperationDeclaration;
import nl.tno.sensorstorm.api.particles.DataParticle;
import nl.tno.sensorstorm.api.particles.DataParticleBatch;
import nl.tno.sensorstorm.storm.SensorStormBolt;

/**
 * A ParticleBatchOperation performs the processing of particles in a
 * {@link DataParticleBatch}, which is produced by a {@link Batcher}. A
 * {@link Batcher} and a {@link ParticleBatchOperation} run together on a
 * {@link SensorStormBolt}. Optionally there can be separate instances of the
 * {@link Batcher} and the {@link ParticleBatchOperation} on the same
 * {@link SensorStormBolt} for different values of a field (see
 * {@link SensorStormBolt}).
 * <p>
 * A {@link ParticleBatchOperation} must have an {@link OperationDeclaration}
 * annotation.
 */
public interface ParticleBatchOperation extends Operation {

	/**
	 * A new {@link DataParticleBatch} has arrived for this operation. A list
	 * containing zero or more particles can be returned to be sent further up
	 * into the topology.
	 * 
	 * @param inputParticleBatch
	 *            A batch of one or more data particle to be processed
	 * @return Returns a list of zero or more data particles to be sent further
	 *         up to the topology (null is also allowed)
	 * @throws OperationException
	 *             when an error occurs in the Operation
	 */
	List<? extends DataParticle> execute(DataParticleBatch inputParticleBatch)
			throws OperationException;

}
