package nl.tno.timeseries.config;

import java.util.Map;

import nl.tno.storm.configuration.api.StormConfiguration;
import nl.tno.storm.configuration.api.StormConfigurationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FetcherConfigManager extends ConfigManager {
	private final Logger logger = LoggerFactory
			.getLogger(FetcherConfigManager.class);

	private final StormConfiguration stormConfiguration;
	private final String fetcherName;

	/**
	 * Create a new config manager for a specific fetcher instance. Each fetcher
	 * instance should have its own config manager. The fetcherName must be
	 * exactly the name as is used in zookeeper. The config is loaded from the
	 * /topologies/[topologyname]/conf/fetchers/[fetcherName]
	 * 
	 * @param stormConfiguration
	 * @param fetcherName
	 */
	public FetcherConfigManager(StormConfiguration stormConfiguration,
			String fetcherName) {
		this.stormConfiguration = stormConfiguration;
		this.fetcherName = fetcherName;

		try {
			stormConfiguration.registerFetcherConfigurationListener(
					fetcherName, this);
		} catch (StormConfigurationException e) {
			logger.error("Can not connect to zookeeper to get Storm configuration for fetcher "
					+ fetcherName + ". Reason: " + e.getMessage());
		}
	}

	/**
	 * After one or more parameters are registered, optionally the config can be
	 * preloaded by calling this method.
	 * 
	 * @throws StormConfigurationException
	 */
	public void loadParameters() throws StormConfigurationException {
		Map<String, String> fetcherConfiguration = stormConfiguration
				.getFetcherConfiguration(fetcherName);
		configurationChanged(fetcherConfiguration);
	}

}