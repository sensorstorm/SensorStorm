package nl.tno.storm.configuration.impl;

import java.util.HashMap;
import java.util.Map;

import nl.tno.storm.configuration.api.ConfigurationListener;
import nl.tno.storm.configuration.api.ExternalStormConfiguration;
import nl.tno.storm.configuration.api.StormConfigurationException;

import org.apache.curator.framework.CuratorFramework;

public class ZookeeperStormConfiguration implements ExternalStormConfiguration {

	public static final String PREFIX = "/topologies";

	private final String topologyId;
	private final Map<String, WatcherHelper> watchers = new HashMap<String, WatcherHelper>();
	final CuratorFramework zkClient;

	public ZookeeperStormConfiguration(String topologyId,
			CuratorFramework zkClient) {
		this.topologyId = topologyId;
		this.zkClient = zkClient;
	}

	@Override
	public Map<String, String> getChannelConfiguration(String channelId)
			throws StormConfigurationException {
		try {
			return pathToMap(getChannelPath(channelId));
		} catch (Exception e) {
			throw new StormConfigurationException("ZooKeeper Exception", e);
		}
	}

	private String getChannelPath(String channelId) {
		return PREFIX + "/" + topologyId + "/conf/channels/" + channelId;
	}

	@Override
	public Map<String, String> getTaskConfiguration(String taskId)
			throws StormConfigurationException {
		try {
			return pathToMap(getTaskPath(taskId));
		} catch (Exception e) {
			throw new StormConfigurationException("ZooKeeper Exception", e);
		}
	}

	private String getTaskPath(String taskId) {
		return PREFIX + "/" + topologyId + "/conf/tasks/" + taskId;
	}

	@Override
	public Map<String, String> getTopologyConfiguration()
			throws StormConfigurationException {
		try {
			return pathToMap(getTopologyPath());
		} catch (Exception e) {
			throw new StormConfigurationException("ZooKeeper Exception", e);
		}
	}

	private String getTopologyPath() {
		return PREFIX + "/" + topologyId + "/conf/topology";
	}

	private synchronized WatcherHelper getWatcherHelper(String mapPath)
			throws Exception {
		if (!watchers.containsKey(mapPath)) {
			watchers.put(mapPath, new WatcherHelper(this, mapPath));
		}
		return watchers.get(mapPath);
	}

	Map<String, String> pathToMap(String path) throws Exception {
		Map<String, String> res = new HashMap<String, String>();
		if (zkClient.checkExists().forPath(path) != null) {
			for (String key : zkClient.getChildren().forPath(path)) {
				byte[] data = zkClient.getData().forPath(path + "/" + key);
				if (data != null) {
					res.put(key, new String(data));
				}
			}
		}
		return res;
	}

	@Override
	public void registerChannelConfigurationListener(String channelId,
			ConfigurationListener listener) throws StormConfigurationException {
		try {
			getWatcherHelper(getChannelPath(channelId)).addListener(listener);
		} catch (Exception e) {
			throw new StormConfigurationException(e);
		}
	}

	@Override
	public void registerTaskConfigurationListener(String taskId,
			ConfigurationListener listener) throws StormConfigurationException {
		try {
			getWatcherHelper(getTaskPath(taskId)).addListener(listener);
		} catch (Exception e) {
			throw new StormConfigurationException(e);
		}
	}

	@Override
	public void registerTopologyConfigurationListener(
			ConfigurationListener listener) throws StormConfigurationException {
		try {
			getWatcherHelper(getTopologyPath()).addListener(listener);
		} catch (Exception e) {
			throw new StormConfigurationException(e);
		}
	}

	@Override
	public void unregisterChannelConfigurationListener(String channelId,
			ConfigurationListener listener) throws StormConfigurationException {
		try {
			getWatcherHelper(getChannelPath(channelId))
					.removeListener(listener);
		} catch (Exception e) {
			throw new StormConfigurationException(e);
		}
	}

	@Override
	public void unregisterTaskConfigurationListener(String taskId,
			ConfigurationListener listener) throws StormConfigurationException {
		try {
			getWatcherHelper(getTaskPath(taskId)).removeListener(listener);
		} catch (Exception e) {
			throw new StormConfigurationException(e);
		}
	}

	@Override
	public void unregisterTopologyConfigurationListener(
			ConfigurationListener listener) throws StormConfigurationException {
		try {
			getWatcherHelper(getTopologyPath()).removeListener(listener);
		} catch (Exception e) {
			throw new StormConfigurationException(e);
		}
	}

}
