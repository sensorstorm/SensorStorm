package nl.tno.sensortimeseries.utils;

import nl.tno.timeseries.TSConfig;

public class TSSensorConfig extends TSConfig {

	private static final long serialVersionUID = -3866119036490531607L;

	/**
	 * <b>Boolean (default = false)</b> configuration parameter indicating if the spout must use measurement time or server time.
	 */
	public static final String BASESENSOR_SPOUT_USE_MEASUREMENT_TIME = "basesensor.spout.usemeasurementtime";

	
	/**
	 * <b>Long (default = 0)</b> configuration parameter setting the main timertick freq in ms. When 0 no timerticks will be emitted.
	 */
	public static final String BASESENSOR_SPOUT_MAIN_TIMERTICK_FREQ = "basesensor.spout.maintimertickfreq";

	
	
}
