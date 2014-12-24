package nl.tno.storm.configuration.api;

import java.util.Map;

public interface ExternalStormConfiguration {

	public Map<String, String> getTopologyConfiguration()
			throws StormConfigurationException;

	public void registerTopologyConfigurationListener(
			ConfigurationListener listener) throws StormConfigurationException;

	public void unregisterTopologyConfigurationListener(
			ConfigurationListener listener) throws StormConfigurationException;

	public Map<String, String> getTaskConfiguration(String taskId)
			throws StormConfigurationException;

	public void registerTaskConfigurationListener(String taskId,
			ConfigurationListener listener) throws StormConfigurationException;

	public void unregisterTaskConfigurationListener(String taskId,
			ConfigurationListener listener) throws StormConfigurationException;

	public Map<String, String> getChannelConfiguration(String channelId)
			throws StormConfigurationException;

	public void registerChannelConfigurationListener(String channelId,
			ConfigurationListener listener) throws StormConfigurationException;

	public void unregisterChannelConfigurationListener(String channelId,
			ConfigurationListener listener) throws StormConfigurationException;

}
