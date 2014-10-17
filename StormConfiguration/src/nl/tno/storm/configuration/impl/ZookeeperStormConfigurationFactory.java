package nl.tno.storm.configuration.impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import nl.tno.storm.configuration.api.StormConfigurationException;
import nl.tno.storm.configuration.api.StormConfigurationFactory;
import nl.tno.storm.configuration.api.ExternalStormConfiguration;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class ZookeeperStormConfigurationFactory implements
		StormConfigurationFactory {
	public static String CONNECTING_STRING = "config.zookeeper.connectionstring";
	public static String TOPOLOGY_NAME = "config.zookeeper.topologyname";

	private static ZookeeperStormConfigurationFactory instance = new ZookeeperStormConfigurationFactory();

	public static ZookeeperStormConfigurationFactory getInstance() {
		return instance;
	}

	private ZookeeperStormConfigurationFactory() {
		// mag niet!
	}

	private CuratorFramework zkClient;
	private final Map<String, ExternalStormConfiguration> stormConfigurations = new HashMap<String, ExternalStormConfiguration>();

	@Override
	public synchronized ExternalStormConfiguration getStormConfiguration(
			String topologyId, String connectionString)
			throws StormConfigurationException {
		try {
			if (zkClient == null) {
				zkClient = CuratorFrameworkFactory.newClient(connectionString,
						new ExponentialBackoffRetry(1000, 3));
				zkClient.start();
			}
			if (!stormConfigurations.containsKey(topologyId)) {
				stormConfigurations.put(topologyId,
						new ZookeeperStormConfiguration(topologyId, zkClient));
			}
			return stormConfigurations.get(topologyId);
		} catch (Exception e) {
			throw new StormConfigurationException(
					"ZooKeeper connection problem", e);
		}
	}

	@Override
	public synchronized ExternalStormConfiguration getStormConfiguration(
			@SuppressWarnings("rawtypes") Map stormNativeConfig)
			throws StormConfigurationException {
		String connectionString = (String) stormNativeConfig
				.get(CONNECTING_STRING);
		String topologyName = (String) stormNativeConfig.get(TOPOLOGY_NAME);

		ExternalStormConfiguration stormConfig = ZookeeperStormConfigurationFactory
				.getInstance().getStormConfiguration(topologyName,
						connectionString);
		return stormConfig;

	}

	@Override
	public synchronized void close() throws IOException {
		zkClient.close();
		zkClient = null;
	}

}
