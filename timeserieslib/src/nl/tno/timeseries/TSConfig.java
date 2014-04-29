package nl.tno.timeseries;


import backtype.storm.Config;

/**
 * Defines the configuration parameters used by StormCV. All references to locations and files require a prefix 
 * indicating the {@link FileAdaptor} to be used. Other adaptors can be registered at {@Link FileFrameFetcher} and {@link StreamWriterOperation}
 * to support other file locations
 * <ul>
 * <li>file:// - is used to point to the local filesystem (typically used in local modes for testing)</li>
 * <li>s3:// - is used to point to an AWS S3 location as follows: S3://bucket/key</li>
 * </ul>
 * 
 * @author Corne Versloot
 *
 */
public class TSConfig extends Config{

	private static final long serialVersionUID = -1316451968047879377L;

	/**
	 * <b>Boolean (default = false)</b> configuration parameter indicating if the spout must cache emitted tuples so they can be replayed
	 */
	public static final String STORMCV_SPOUT_FAULTTOLERANT = "stormcv.spout.faulttolerant";
	
	/**
	 * <b>Integer (default = 30)</b> configuration parameter setting the maximum time to live for items being cached within the topology (both spouts and bolts use this configuration)
	 */
	public static final String STORMCV_CACHES_TIMEOUT_SEC = "stormcv.caches.timeout";
	
	/**
	 * <b>Integer (default = 500)</b> configuration parameter setting the maximum number of elements being cached by spouts and bolts (used to avoid memory overload) 
	 */
	public static final String STORMCV_CACHES_MAX_SIZE = "stormcv.caches.maxsize";
	
	
}
