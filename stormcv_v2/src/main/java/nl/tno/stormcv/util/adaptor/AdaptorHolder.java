package nl.tno.stormcv.util.adaptor;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import nl.tno.stormcv.StormCVConfig;
import nl.tno.timeseries.interfaces.Fetcher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility class used to hold and manage @link {@link FileAdaptor} objects registered in the StormCVConfig. Adaptors are used
 * to interact with remote filesystems (S3, FTP etc) and are typically used by @link {@link Fetcher}s. 
 * An AdaptorHolder can be constructed providing the StormCVConfig and should be used as follows:
 * <pre>
 * {@code
 * AdaptorHolder adaptorHolder = new AdaptorHolder(StormCVConfig);
 * String location = "ftp://domain.nl/path/to/file";
 * FileAdaptor adaptor = adaptorHolder.getAdaptor(location);
 * adaptor.moveTo(location);
 * File localFile = adaptor.getAsFile();
 * }
 * </pre>
 *  
 * @author Corne Versloot
 *
 */
public class AdaptorHolder {

	private HashMap<String, FileAdaptor> adaptors;
	Logger logger = LoggerFactory.getLogger(AdaptorHolder.class);
	
	/**
	 * Uses the provided stormConf to instantiate and configure registered FileAdaptors
	 * using {@link StormCVConfig}.STORMCV_ADAPTORS
	 * @param stormConf
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public AdaptorHolder(Map stormConf){
		adaptors = new HashMap<String, FileAdaptor>();
		List<String> registered = (List<String>)stormConf.get(StormCVConfig.STORMCV_ADAPTORS);
		for(String name : registered) try{
			FileAdaptor adaptor = (FileAdaptor)Class.forName(name).newInstance();
			adaptor.prepare(stormConf);
			adaptors.put(adaptor.getProtocol(), adaptor);
		}catch(Exception e){
			logger.warn("Unable to instantiate FileAdaptor: "+name+" due to: "+e.getMessage());
		}
	}
	
	public FileAdaptor getAdaptor(String location){
		try {
			return adaptors.get(new URI(location).getScheme());
		} catch (URISyntaxException e) {
			logger.warn(location+" does hot have valid URI syntax: "+e.getMessage());
		}
		return null;
	}
	
}
