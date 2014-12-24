package nl.tno.sensorstorm.operations;

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
 */
public interface Operation extends Serializable {

	/**
	 * Initialize this operation.
	 * 
	 * @param fieldGrouperValue
	 *            Value for the grouping field if any, otherwise null
	 * @param startTimeStamp
	 *            The time of the first particle this operation will get. Or -1
	 *            if a single operation is created for all particles (in the
	 *            SensorStormBolt constructor the fieldgrouperid == null)
	 * @param stormNativeConfig
	 *            A reference to the storm config object
	 * @param zookeeperStormConfiguration
	 *            A reference to the {@link ExternalStormConfiguration}
	 * @throws OperationException
	 *             If an error occurs in the {@link Operation}
	 * 
	 */
	void init(String fieldGrouperValue, long startTimeStamp,
			@SuppressWarnings("rawtypes") Map stormNativeConfig,
			ExternalStormConfiguration zookeeperStormConfiguration)
			throws OperationException;

}
