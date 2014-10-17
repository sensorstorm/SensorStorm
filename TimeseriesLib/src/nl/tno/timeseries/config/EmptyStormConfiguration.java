package nl.tno.timeseries.config;

import java.util.HashMap;
import java.util.Map;

import nl.tno.storm.configuration.api.ConfigurationListener;
import nl.tno.storm.configuration.api.ExternalStormConfiguration;
import nl.tno.storm.configuration.api.StormConfigurationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmptyStormConfiguration implements ExternalStormConfiguration {
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

}
