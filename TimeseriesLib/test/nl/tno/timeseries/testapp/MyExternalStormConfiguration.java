package nl.tno.timeseries.testapp;

import java.util.HashMap;
import java.util.Map;

import nl.tno.storm.configuration.api.ConfigurationListener;
import nl.tno.storm.configuration.api.ExternalStormConfiguration;
import nl.tno.storm.configuration.api.StormConfigurationException;

public class MyExternalStormConfiguration implements ExternalStormConfiguration {

	public MyExternalStormConfiguration() {
		System.out.println("--- LOCAL CONFIG CREATED ---");
	}

	@Override
	public Map<String, String> getTopologyConfiguration()
			throws StormConfigurationException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void registerTopologyConfigurationListener(
			ConfigurationListener listener) throws StormConfigurationException {
		// TODO Auto-generated method stub

	}

	@Override
	public void unregisterTopologyConfigurationListener(
			ConfigurationListener listener) throws StormConfigurationException {
		// TODO Auto-generated method stub

	}

	@Override
	public Map<String, String> getTaskConfiguration(String taskId)
			throws StormConfigurationException {

		if (taskId.equals("myconfigoperation")) {
			HashMap<String, String> hashMap = new HashMap<String, String>();
			hashMap.put("myvar1", "1200");
			hashMap.put("myvar2", "1.2");
			hashMap.put("myvar3", "Hello");
			return hashMap;
		} else {
			return null;
		}
	}

	@Override
	public void registerTaskConfigurationListener(String taskId,
			ConfigurationListener listener) throws StormConfigurationException {
		// TODO Auto-generated method stub

	}

	@Override
	public void unregisterTaskConfigurationListener(String taskId,
			ConfigurationListener listener) throws StormConfigurationException {
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
