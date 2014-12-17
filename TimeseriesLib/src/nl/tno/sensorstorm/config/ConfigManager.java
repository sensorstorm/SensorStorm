package nl.tno.sensorstorm.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicReference;

import nl.tno.storm.configuration.api.ConfigurationListener;
import nl.tno.storm.configuration.api.StormConfigurationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ConfigManager implements ConfigurationListener {
	private final Logger logger = LoggerFactory.getLogger(ConfigManager.class);

	private final Map<String, AtomicReference<?>> keyValueContainers = new HashMap<String, AtomicReference<?>>();

	/**
	 * Register a parameter to be linked to a zookeeper configuration. The key
	 * must match the key name in zookeeper. The value must be stored in a
	 * AtomicReference due to the threading issues between zookeeper and storm.
	 * 
	 * @param key
	 * @param keyValueContainer
	 * @throws StormConfigurationException
	 */
	public void registerParameter(String key,
			AtomicReference<?> keyValueContainer)
			throws StormConfigurationException {
		if (keyValueContainer == null) {
			throw new StormConfigurationException(
					"keyValueContainer may not be null.");
		}
		if (canHandleAtomicReference(keyValueContainer)) {
			keyValueContainers.put(key, keyValueContainer);
		} else {
			throw new StormConfigurationException(
					"Value type is unsupported (must be a String, Double, Long)");
		}
	}

	/**
	 * Unregister a parameter, so that its AtomicReference will no longer be
	 * updated.
	 * 
	 * @param key
	 */
	public void unregisterParameter(String key) {
		keyValueContainers.remove(key);
	}

	@Override
	public void configurationChanged(Map<String, String> newConfiguration) {
		for (Entry<String, String> keyValueContainer : newConfiguration
				.entrySet()) {
			setKeyValueContainer(keyValueContainer.getKey(),
					keyValueContainer.getValue());
		}
	}

	private boolean canHandleAtomicReference(
			AtomicReference<?> keyValueContainer) {
		if (keyValueContainer.get() instanceof Long) {
			return true;
		} else if (keyValueContainer.get() instanceof Double) {
			return true;
		} else if (keyValueContainer.get() instanceof String) {
			return true;
		} else {
			return false;
		}
	}

	@SuppressWarnings("unchecked")
	private void setKeyValueContainer(String key, String value) {
		AtomicReference<?> keyValueContainer = keyValueContainers.get(key);
		if (keyValueContainer == null)
			return;

		try {
			if (keyValueContainer.get() instanceof Long) {
				((AtomicReference<Long>) keyValueContainer).set(Long
						.parseLong(value));
			} else if (keyValueContainer.get() instanceof Double) {
				((AtomicReference<Double>) keyValueContainer).set(Double
						.parseDouble(value));
			} else if (keyValueContainer.get() instanceof String) {
				((AtomicReference<String>) keyValueContainer).set(value);
			} else {
				logger.error("Can not convert configuration parameter [" + key
						+ ", " + value + "] into its container");
			}
		} catch (NumberFormatException e) {
			logger.error("Can not set config parameter " + key + " of type "
					+ keyValueContainer.get().getClass().getName()
					+ " to value " + value + " msg=" + e);
		}
	}

}
