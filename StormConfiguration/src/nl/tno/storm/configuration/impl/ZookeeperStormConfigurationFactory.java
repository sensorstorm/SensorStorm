package nl.tno.storm.configuration.impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import nl.tno.storm.configuration.api.StormConfiguration;
import nl.tno.storm.configuration.api.StormConfigurationException;
import nl.tno.storm.configuration.api.StormConfigurationFactory;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class ZookeeperStormConfigurationFactory implements StormConfigurationFactory {

	private static ZookeeperStormConfigurationFactory instance = new ZookeeperStormConfigurationFactory();

	public static ZookeeperStormConfigurationFactory getInstance() {
		return instance;
	}

	private ZookeeperStormConfigurationFactory() {
		// mag niet!
	}

	private CuratorFramework zkClient;
	private final Map<String, StormConfiguration> stormConfigurations = new HashMap<String, StormConfiguration>();

	@Override
	public synchronized StormConfiguration getStormConfiguration(String topologyId, String connectionString)
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
	public synchronized StormConfiguration getStormConfiguration(@SuppressWarnings("rawtypes") Map config)
			throws StormConfigurationException {
		// TODO zookeeper connection string uit map halen
		System.out.println(config);
		throw new UnsupportedOperationException();
	}

	@Override
	public synchronized void close() throws IOException {
		zkClient.close();
		zkClient = null;
	}

}
