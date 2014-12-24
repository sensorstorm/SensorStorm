package nl.tno.sensorstorm.api.processing;

import java.util.List;

import nl.tno.sensorstorm.api.annotation.OperationDeclaration;
import nl.tno.sensorstorm.api.particles.DataParticle;
import nl.tno.sensorstorm.storm.SensorStormBolt;

/**
 * A SingleParticleOperation performs the processing of individual
 * {@link DataParticle}s. It runs on top of a {@link SensorStormBolt}.
 * Optionally there can be separate instances {@link SingleParticleOperation} on
 * the same {@link SensorStormBolt} for different values of a field (see
 * {@link SensorStormBolt}).
 * 
 * A {@link SingleParticleOperation} must have an {@link OperationDeclaration}
 * annotation.
 */
public interface SingleParticleOperation extends Operation {

	/**
	 * A new particle has arrived for this operation. A list containing zero or
	 * more particles can be returned to be sent further up into the topology.
	 * 
	 * @param inputParticle
	 *            The dataParticle that need to be processed.
	 * @return Returns a list of zero or more data particles to be sent further
	 *         up to the topology (null is also allowed)
	 * @throws OperationException
	 *             when an error occurs in the Operation
	 */
	List<? extends DataParticle> execute(DataParticle inputParticle)
			throws OperationException;

}
