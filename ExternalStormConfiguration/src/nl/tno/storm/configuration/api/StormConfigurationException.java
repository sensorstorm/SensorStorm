package nl.tno.storm.configuration.api;

public class StormConfigurationException extends Exception {

	private static final long serialVersionUID = 1L;

	public StormConfigurationException(String msg) {
		super(msg);
	}

	public StormConfigurationException(String msg, Exception e) {
		super(msg, e);
	}

	public StormConfigurationException(Exception e) {
		super(e);
	}

}
