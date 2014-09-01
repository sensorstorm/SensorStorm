package nl.tno.storm.configuration.api;

import java.util.Map;

public interface ConfigurationListener {

	public void configurationChanged(Map<String, String> newConfiguration);

}
