package nl.tno.sensorstorm.config;

import java.util.Map;

import nl.tno.storm.configuration.api.ExternalStormConfiguration;
import nl.tno.storm.configuration.api.StormConfigurationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OperationConfigManager extends ConfigManager {
	private final Logger logger = LoggerFactory
			.getLogger(OperationConfigManager.class);

	private final ExternalStormConfiguration stormConfiguration;
	private final String operationName;

	/**
	 * Create a new config manager for a specific operation instance. Each
	 * operation instance should have its own config manager. The operationName
	 * must be exactly the name as is used in zookeeper. The config is loaded
	 * from the /topologies/[topologyname]/conf/operations/[operationName]
	 * 
	 * @param stormConfiguration
	 * @param operationName
	 */
	public OperationConfigManager(
			ExternalStormConfiguration stormConfiguration, String operationName) {
		this.stormConfiguration = stormConfiguration;
		this.operationName = operationName;

		try {
			stormConfiguration.registerTaskConfigurationListener(operationName,
					this);
		} catch (StormConfigurationException e) {
			logger.error("Can not connect to zookeeper to get Storm configuration for operation "
					+ operationName + ". Reason: " + e.getMessage());
		}
	}

	/**
	 * After one or more parameters are registered, optionally the config can be
	 * preloaded by calling this method.
	 * 
	 * @throws StormConfigurationException
	 */
	public void loadParameters() throws StormConfigurationException {
		Map<String, String> operationConfiguration = stormConfiguration
				.getTaskConfiguration(operationName);
		configurationChanged(operationConfiguration);
	}

}
