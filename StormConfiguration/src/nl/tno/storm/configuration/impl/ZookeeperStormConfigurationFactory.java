package nl.tno.storm.configuration.impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import nl.tno.storm.configuration.api.ZookeeperStormConfigurationAPI;
import nl.tno.storm.configuration.api.StormConfigurationException;
import nl.tno.storm.configuration.api.StormConfigurationFactory;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZookeeperStormConfigurationFactory implements StormConfigurationFactory {
	public static String CONNECTING_STRING = "config.zookeeper.connectionstring";
	public static String TOPOLOGY_NAME = "config.zookeeper.topologyname";
	private static Logger logger = LoggerFactory.getLogger(ZookeeperStormConfigurationFactory.class);

	private static ZookeeperStormConfigurationFactory instance = new ZookeeperStormConfigurationFactory();

	public static ZookeeperStormConfigurationFactory getInstance() {
		return instance;
	}

	private ZookeeperStormConfigurationFactory() {
		// mag niet!
	}

	private CuratorFramework zkClient;
	private final Map<String, ZookeeperStormConfigurationAPI> stormConfigurations = new HashMap<String, ZookeeperStormConfigurationAPI>();

	@Override
	public synchronized ZookeeperStormConfigurationAPI getStormConfiguration(String topologyId, String connectionString)
			throws StormConfigurationException {
		try {
			if (zkClient == null) {
				zkClient = CuratorFrameworkFactory.newClient(connectionString, new ExponentialBackoffRetry(1000, 3));
				zkClient.start();
			}
			if (!stormConfigurations.containsKey(topologyId)) {
				stormConfigurations.put(topologyId, new ZookeeperStormConfiguration(topologyId, zkClient));
			}
			return stormConfigurations.get(topologyId);
		} catch (Exception e) {
			throw new StormConfigurationException("ZooKeeper connection problem", e);
		}
	}

	@Override
	public synchronized ZookeeperStormConfigurationAPI getStormConfiguration(@SuppressWarnings("rawtypes") Map stormNativeConfig)
			throws StormConfigurationException {
		String connectionString = (String) stormNativeConfig
				.get(CONNECTING_STRING);
		String topologyName = (String) stormNativeConfig.get(TOPOLOGY_NAME);

		ZookeeperStormConfigurationAPI stormConfig =  ZookeeperStormConfigurationFactory
				.getInstance().getStormConfiguration(topologyName,
						connectionString);
		stormConfig.setNativeStormConfig(stormNativeConfig);
		return stormConfig;

	}

	@Override
	public synchronized void close() throws IOException {
		zkClient.close();
		zkClient = null;
	}

}
