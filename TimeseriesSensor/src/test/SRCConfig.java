package test;

import nl.tno.sensortimeseries.utils.TSSensorConfig;

public class SRCConfig extends TSSensorConfig {
	private static final long serialVersionUID = 8422467403835839518L;

	
	/**
	 * <b>Long (default = 1000)</b> configuration parameter setting the dmax of sampleratechangebility
	 */
	public static final String SAMPLERATE_CHANGEABILITY_DMAX = "sampleratechangeability.dmax";

	/**
	 * <b>Long (default = 100)</b> configuration parameter setting the dmax of sampleratechangebility
	 */
	public static final String SAMPLERATE_CHANGEABILITY_DMIN = "sampleratechangeability.dmin";

	/**
	 * <b>Double (default = 1.1)</b> configuration parameter setting the dmax of sampleratechangebility
	 */
	public static final String SAMPLERATE_CHANGEABILITY_ALPHA = "sampleratechangeability.alpha";


	public static final String LIFEDIJK_FILENAME = "lifedijk.filename";

	
}
