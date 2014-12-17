package nl.tno.storm.configuration.impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import nl.tno.storm.configuration.api.ExternalStormConfiguration;
import nl.tno.storm.configuration.api.StormConfigurationException;
import nl.tno.storm.configuration.api.StormConfigurationFactory;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class ZookeeperStormConfigurationFactory implements
		StormConfigurationFactory {
	public static String CONNECTING_STRING = "config.zookeeper.connectionstring";
	public static String TOPOLOGY_NAME = "config.zookeeper.topologyname";

	private static ExternalStormConfiguration externalStormConfiguration = null;
	private static ZookeeperStormConfigurationFactory instance = new ZookeeperStormConfigurationFactory();

	public static ZookeeperStormConfigurationFactory getInstance() {
		return instance;
	}

	private ZookeeperStormConfigurationFactory() {
		// mag niet!
	}

	public static void setExternalStormConfiguration(
			ExternalStormConfiguration newExternalStormConfiguration) {
		externalStormConfiguration = newExternalStormConfiguration;
	}

	private CuratorFramework zkClient;
	private final Map<String, ExternalStormConfiguration> stormConfigurations = new HashMap<String, ExternalStormConfiguration>();

	@Override
	public synchronized ExternalStormConfiguration getStormConfiguration(
			String topologyId, String connectionString)
			throws StormConfigurationException {
		try {
			if (!stormConfigurations.containsKey(topologyId)) {
				// default externalStormConfiguration or other?
				if (externalStormConfiguration == null) {
					// zookeeper connection available
					if (zkClient == null) {
						zkClient = CuratorFrameworkFactory.newClient(
								connectionString, new ExponentialBackoffRetry(
										1000, 3));
						zkClient.start();
					}
					externalStormConfiguration = new ZookeeperStormConfiguration(
							topologyId, zkClient);
				}
				stormConfigurations.put(topologyId, externalStormConfiguration);
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
		if (zkClient != null) {
			zkClient.close();
		}
		zkClient = null;
	}

}
