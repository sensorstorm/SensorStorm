package nl.tno.timeseries.operations;

import java.io.Serializable;
import java.util.Map;

import nl.tno.storm.configuration.api.ExternalStormConfiguration;

/**
 * An operation performs the processing of particles in a fieldGroupValue. This
 * is the abstract interface or the SingleOperation and the BatchOperation The
 * SensorStormBolt manages the operations, each fieldGroupValue will have its
 * own operation instance. An operation is created at soon as the
 * SensorStormBolt gets a particle with an unknown fieldGroupValue.
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
	 * @param fieldGrouperValue
	 * @param stormNativeConfig
	 *            A reference to the storm config object
	 * @param zookeeperStormConfiguration
	 *            A reference to the zookeeper storm config api
	 */
	public void init(String fieldGrouperValue,
			@SuppressWarnings("rawtypes") Map stormNativeConfig,
			ExternalStormConfiguration zookeeperStormConfiguration)
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
