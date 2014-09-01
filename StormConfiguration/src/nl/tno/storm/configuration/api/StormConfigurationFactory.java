package nl.tno.storm.configuration.api;

import java.io.Closeable;
import java.util.Map;

public interface StormConfigurationFactory extends Closeable {

	/**
	 * Get the StormConfiguration for a specific topology based on the ToplogyId
	 * and the Zookeeper ConnectionString
	 * 
	 * @param topologyId
	 *            toplogyId as used in the configuration
	 * @param connectionString
	 *            Zookeeper connection string
	 * @return StormConfiguration instance for this topology
	 * @throws StormConfigurationException
	 *             Probably something is wrong with Zookeeper
	 */
	public StormConfiguration getStormConfiguration(String topologyId, String connectionString)
			throws StormConfigurationException;

	/**
	 * Get the StormConfiguration for a specific topology based on the Storm
	 * configuration map
	 * 
	 * This is a convenience method with gets the necessary information from the
	 * Storm configuration map.
	 * 
	 * @param Map
	 *            Storm configuration map
	 * @return StormConfiguration instance for this topology
	 * @throws StormConfigurationException
	 *             Probably something is wrong with Zookeeper
	 */
	public StormConfiguration getStormConfiguration(@SuppressWarnings("rawtypes") Map config)
			throws StormConfigurationException;

}
