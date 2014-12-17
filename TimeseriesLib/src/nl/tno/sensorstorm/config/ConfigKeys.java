package nl.tno.sensorstorm.config;

import java.util.Map;

import backtype.storm.Config;

public class ConfigKeys {

	// settings in the native storm config:
	//
	// For throttleling:
	// set Storm native param Config.TOPOLOGY_MAX_SPOUT_PENDING to a number
	// greater than 0
	//
	// For Fault tollerant:
	// set the ConfigKeys.TOPOLOGY_FAULT_TOLERANT parameter to
	// true
	// set Storm native param Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS to a useful
	// value, default is 30 seconds
	// set the ConfigKeys.TOPOLOGY_TUPLECACHE_MAX_SIZE parameter to a useful
	// value, default is 500 tuples

	// ****
	// **** labels and default values for library specific parameters ****
	// ****
	// maximum number of elements in the caches within the topology
	public static final String SPOUT_TUPLECACHE_MAX_SIZE = "spout.tuplecache.maxsize";
	public static final int SPOUT_TUPLECACHE_MAX_SIZE_DEFAULT = 500;

	// anchor, ack en fail tuples in the topology
	public static final String TOPOLOGY_FAULT_TOLERANT = "topology.fault-tolerant";
	public static final boolean TOPOLOGY_FAULT_TOLERANT_DEFAULT = false;

	// ****
	// **** default values for Storm native parameters ****
	// ****
	// TTL (seconds) for all elements in all caches throughout the topology.
	// This is the default value for storm native parameter
	// Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS
	public static final int TOPOLOGY_MESSAGE_TIMEOUT_SECS_DEFAULT = 30;

	/**
	 * Helper method to get a configuration value or initialize it with a
	 * default value.
	 * 
	 * @param stormNativeConfig
	 * @param key
	 * @param defaultValue
	 * @return
	 */
	public static int getStormNativeIntegerConfigValue(
			@SuppressWarnings("rawtypes") Map stormNativeConfig, String key,
			int defaultValue) {
		Object object = stormNativeConfig
				.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS);
		if (object == null) {
			return defaultValue;
		}
		if (object instanceof Integer) {
			return ((Integer) stormNativeConfig
					.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS)).intValue();
		} else {
			return defaultValue;
		}
	}

}
