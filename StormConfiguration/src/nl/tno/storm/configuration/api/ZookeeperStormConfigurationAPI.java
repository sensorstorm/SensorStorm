package nl.tno.storm.configuration.api;

import java.util.Map;

public interface ZookeeperStormConfigurationAPI {

	public Map<String, String> getTopologyConfiguration() throws StormConfigurationException;

	public void registerTopologyConfigurationListener(ConfigurationListener listener)
			throws StormConfigurationException;

	public void unregisterTopologyConfigurationListener(ConfigurationListener listener)
			throws StormConfigurationException;

	
	
	public Map<String, String> getTaskConfiguration(String taskId) throws StormConfigurationException;

	public void registerTaskConfigurationListener(String taskId, ConfigurationListener listener)
			throws StormConfigurationException;

	public void unregisterTaskConfigurationListener(String taskId, ConfigurationListener listener)
			throws StormConfigurationException;


	public Map<String, String> getChannelConfiguration(String channelId) throws StormConfigurationException;

	public void registerChannelConfigurationListener(String channelId, ConfigurationListener listener)
			throws StormConfigurationException;

	public void unregisterChannelConfigurationListener(String channelId, ConfigurationListener listener)
			throws StormConfigurationException;

	
	//------------------------------- hieronder niet nmeer ---------------------
	
	public Map<String, String> getOperationConfiguration(String operationId) throws StormConfigurationException;

	public void registerOperationConfigurationListener(String operationId, ConfigurationListener listener)
			throws StormConfigurationException;

	public void unregisterOperationConfigurationListener(String operationId, ConfigurationListener listener)
			throws StormConfigurationException;

	
	public Map<String, String> getFetcherConfiguration(String fetcherId) throws StormConfigurationException;

	public void registerFetcherConfigurationListener(String fetcherId, ConfigurationListener listener)
			throws StormConfigurationException;

	public void unregisterFetcherConfigurationListener(String fetcherId, ConfigurationListener listener)
			throws StormConfigurationException;

	public Map<String, String> getStreamConfiguration(String streamId) throws StormConfigurationException;

	public void registerStreamConfigurationListener(String streamId, ConfigurationListener listener)
			throws StormConfigurationException;

	public void unregisterStreamConfigurationListener(String streamId, ConfigurationListener listener)
			throws StormConfigurationException;

	

	@SuppressWarnings("rawtypes")
	public void setNativeStormConfig(Map nativeStormConfig);
	
	@SuppressWarnings("rawtypes")
	public Map getNativeStormConfig();

}
