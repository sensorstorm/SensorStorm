package nl.tno.timeseries.config;

import java.util.HashMap;
import java.util.Map;

import nl.tno.storm.configuration.api.ConfigurationListener;
import nl.tno.storm.configuration.api.StormConfiguration;
import nl.tno.storm.configuration.api.StormConfigurationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmptyStormConfiguration implements StormConfiguration {
	private final Logger logger = LoggerFactory
			.getLogger(EmptyStormConfiguration.class);

	@Override
	public Map<String, String> getTopologyConfiguration()
			throws StormConfigurationException {
		logger.error("No connection made to zookeeper, can not get the configuration for topology.");
		return new HashMap<String, String>();
	}

	@Override
	public void registerTopologyConfigurationListener(
			ConfigurationListener listener) throws StormConfigurationException {
		logger.error("No connection made to zookeeper, can not register the configuration for topology.");
	}

	@Override
	public void unregisterTopologyConfigurationListener(
			ConfigurationListener listener) throws StormConfigurationException {
		logger.error("No connection made to zookeeper, can not unregister the configuration for topology.");
	}

	@Override
	public Map<String, String> getTaskConfiguration(String taskId)
			throws StormConfigurationException {
		logger.error("No connection made to zookeeper, can not get the configuration for task "
				+ taskId);
		return new HashMap<String, String>();
	}

	@Override
	public void registerTaskConfigurationListener(String taskId,
			ConfigurationListener listener) throws StormConfigurationException {
		logger.error("No connection made to zookeeper, can not register the configuration for task.");
	}

	@Override
	public void unregisterTaskConfigurationListener(String taskId,
			ConfigurationListener listener) throws StormConfigurationException {
		logger.error("No connection made to zookeeper, can not unregister the configuration for task.");
	}

	@Override
	public Map<String, String> getStreamConfiguration(String streamId)
			throws StormConfigurationException {
		logger.error("No connection made to zookeeper, can not get the configuration for stream "
				+ streamId);
		return new HashMap<String, String>();
	}

	@Override
	public void registerStreamConfigurationListener(String streamId,
			ConfigurationListener listener) throws StormConfigurationException {
		logger.error("No connection made to zookeeper, can not register the configuration for stream.");
	}

	@Override
	public void unregisterStreamConfigurationListener(String streamId,
			ConfigurationListener listener) throws StormConfigurationException {
		logger.error("No connection made to zookeeper, can not unregister the configuration for stream.");
	}

	@Override
	public Map<String, String> getChannelConfiguration(String channelId)
			throws StormConfigurationException {
		logger.error("No connection made to zookeeper, can not get the configuration for channel "
				+ channelId);
		return new HashMap<String, String>();
	}

	@Override
	public void registerChannelConfigurationListener(String channelId,
			ConfigurationListener listener) throws StormConfigurationException {
		logger.error("No connection made to zookeeper, can not register the configuration for channel.");
	}

	@Override
	public void unregisterChannelConfigurationListener(String channelId,
			ConfigurationListener listener) throws StormConfigurationException {
		logger.error("No connection made to zookeeper, can not unregister the configuration for channel.");
	}

	@Override
	public Map<String, String> getOperationConfiguration(String operationId)
			throws StormConfigurationException {
		logger.error("No connection made to zookeeper, can not get the configuration for operation "
				+ operationId);
		return new HashMap<String, String>();
	}

	@Override
	public void registerOperationConfigurationListener(String operationId,
			ConfigurationListener listener) throws StormConfigurationException {
		logger.error("No connection made to zookeeper, can not register the configuration for operation.");
	}

	@Override
	public void unregisterOperationConfigurationListener(String operationId,
			ConfigurationListener listener) throws StormConfigurationException {
		logger.error("No connection made to zookeeper, can not unregister the configuration for operation.");
	}

	@Override
	public Map<String, String> getFetcherConfiguration(String fetcherId)
			throws StormConfigurationException {
		logger.error("No connection made to zookeeper, can not get the configuration for fetcher "
				+ fetcherId);
		return new HashMap<String, String>();
	}

	@Override
	public void registerFetcherConfigurationListener(String fetcherId,
			ConfigurationListener listener) throws StormConfigurationException {
		logger.error("No connection made to zookeeper, can not register the configuration for fetcher.");
	}

	@Override
	public void unregisterFetcherConfigurationListener(String fetcherId,
			ConfigurationListener listener) throws StormConfigurationException {
		logger.error("No connection made to zookeeper, can not unregister the configuration for fetcher.");
	}

}
