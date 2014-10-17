package nl.tno.storm.configuration.impl;

import java.util.Map;

import nl.tno.storm.configuration.api.ConfigurationListener;
import nl.tno.storm.configuration.api.ExternalStormConfiguration;
import nl.tno.storm.configuration.api.StormConfigurationException;

public class LocalStormConfiguration implements ExternalStormConfiguration {

	@Override
	public Map<String, String> getTopologyConfiguration()
			throws StormConfigurationException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void registerTopologyConfigurationListener(
			ConfigurationListener listener) {
		// TODO Auto-generated method stub

	}

	@Override
	public void unregisterTopologyConfigurationListener(
			ConfigurationListener listener) {
		// TODO Auto-generated method stub

	}

	@Override
	public Map<String, String> getTaskConfiguration(String taskId)
			throws StormConfigurationException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void registerTaskConfigurationListener(String taskId,
			ConfigurationListener listener) {
		// TODO Auto-generated method stub

	}

	@Override
	public void unregisterTaskConfigurationListener(String taskId,
			ConfigurationListener listener) {
		// TODO Auto-generated method stub

	}

	@Override
	public Map<String, String> getChannelConfiguration(String channelId)
			throws StormConfigurationException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void registerChannelConfigurationListener(String channelId,
			ConfigurationListener listener) throws StormConfigurationException {
		// TODO Auto-generated method stub

	}

	@Override
	public void unregisterChannelConfigurationListener(String channelId,
			ConfigurationListener listener) throws StormConfigurationException {
		// TODO Auto-generated method stub

	}

}
