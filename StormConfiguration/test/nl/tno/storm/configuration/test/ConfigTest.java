package nl.tno.storm.configuration.test;

import java.util.Map;

import nl.tno.storm.configuration.api.ConfigurationListener;
import nl.tno.storm.configuration.api.ZookeeperStormConfigurationAPI;
import nl.tno.storm.configuration.api.StormConfigurationException;
import nl.tno.storm.configuration.impl.ZookeeperStormConfigurationFactory;

public class ConfigTest implements ConfigurationListener {

	private static final String TOPOLOGY_ID = "test";
	private final ZookeeperStormConfigurationAPI stormConfiguration;

	public ConfigTest() throws StormConfigurationException,
			InterruptedException {
		stormConfiguration = ZookeeperStormConfigurationFactory.getInstance()
				.getStormConfiguration(TOPOLOGY_ID, "134.221.210.122:2181");

		stormConfiguration.registerTopologyConfigurationListener(this);

		while (true) {
			Thread.sleep(5000);
			System.out.println("Current config: "
					+ stormConfiguration.getTopologyConfiguration());
		}

	}

	public static void main(String[] args) throws StormConfigurationException,
			InterruptedException {
		new ConfigTest();
	}

	@Override
	public void configurationChanged(Map<String, String> newConfiguration) {
		System.err.println("Yay, config updated! " + newConfiguration);
	}
}
