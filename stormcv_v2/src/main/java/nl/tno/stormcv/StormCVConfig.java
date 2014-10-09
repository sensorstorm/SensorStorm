package nl.tno.stormcv;

import java.util.ArrayList;

import nl.tno.stormcv.particle.Descriptor;
import nl.tno.stormcv.particle.Feature;
import nl.tno.stormcv.particle.Frame;
import nl.tno.stormcv.particle.serializer.DescriptorSerializer;
import nl.tno.stormcv.particle.serializer.FeatureSerializer;
import nl.tno.stormcv.particle.serializer.FrameSerializer;
import nl.tno.stormcv.util.adaptor.ClasspathAdaptor;
import nl.tno.stormcv.util.adaptor.FileAdaptor;
import nl.tno.stormcv.util.adaptor.FtpAdaptor;
import nl.tno.stormcv.util.adaptor.LocalFileAdaptor;
import nl.tno.stormcv.util.adaptor.S3Adaptor;

import backtype.storm.Config;
import backtype.storm.tuple.Tuple;

/**
 * Defines the configuration parameters used by StormCV. All references to locations and files require a prefix 
 * indicating the {@link FileAdaptor} to be used. If an Adaptor requires credentials they must be set in the configuration as well.
 * <ul>
 * <li>file:// - is used to point to the local filesystem (typically used in local modes for testing)</li>
 * <li>s3:// - is used to point to an AWS S3 location as follows: S3://bucket/key. Credentials must be provided using STORMCV_AWS_S3_KEY / SECRET</li>
 * <li>ftp:// - is used to access a FTP server. Credentials must be provided using STORMCV_FTP_USER / PASS</li>
 * </ul>
 * 
 * @author Corne Versloot
 *
 */
public class StormCVConfig extends Config{

	private static final long serialVersionUID = 6290659199719921212L;

	/**
	 * <b>String (default = NONE)</b> configuration parameter setting the AWS S3 Key to use by the {@link S3Adaptor}
	 */
	public static final String STORMCV_AWS_S3_KEY = "stormcv.s3.key";
	
	/**
	 * <b>String (default = NONE) </b> configuration parameter setting the AWS S3 Secret {@link S3Adaptor}
	 */
	public static final String STORMCV_AWS_S3_SECRET = "stormcv.s3.secret";
	
	/**
	 * <b>List (default = NONE) </b> configuration parameter containing the available {@link FileAdaptor} in the topology
	 */
	public static final String STORMCV_ADAPTORS = "stormcv.adaptors";
	
	/**
	 * <b>String (default = NONE)</b> configuration parameter setting the location to write streams to (used by {@link StreamWriterOperation})
	 */
	public static final String STORMCV_STREAMWRITER_LOCATION = "stormcv.streamwriter.location"; 
	
	/**
	 * <b>Integer (default = 30)</b> configuration parameter setting the maximum idle time in seconds after which the {@link StreamWriterOperation} will close the file
	 */
	public static final String STORMCV_MAXIDLE_SEC = "stormcv.streamwriter.maxidlesecs";
	
	/**
	 * <b>Float (default = 1.0)</b> configuration parameter setting the speed of the file being written by the {@link StreamWriterOperation}. 1.0= normal speed (default), 2.0 = twice the normal speed, 0.5 = half normal speed. 
	 */
	public static final String STORMCV_STREAMWRITER_SPEED = "stormcv.streamwriter.speed";
	
	/**
	 * <b>String</b> configuration parameter setting the username to use by the @link {@link FtpAdaptor}
	 */
	public static final String STORMCV_FTP_USER = "stormcv.ftp.username";

	/**
	 * <b>String</b> configuration parameter setting the password to use by the @link {@link FtpAdaptor}
	 */
	public static final String STORMCV_FTP_PASS = "stormcv.ftp.password";
	
	
	/**
	 * Creates a specific Configuration for StormCV.
	 * <ul>
	 * <li>Sets buffer sizes to 2 to optimize for few large size {@link Tuple}s instead of loads of small sized Tuples</li>
	 * <li>Registers known Kryo serializers for the Particles.</li>
	 * <li>Registers known FileAdaptors. New file adapters can be added through registerAdaptor</li>
	 * </ul>
	 */
	public StormCVConfig(){
		super();
		// ------- Create StormCV specific config -------
		put(Config.TOPOLOGY_RECEIVER_BUFFER_SIZE, 2); // sets the maximum number of messages to batch before sending them to executers
		put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, 2); // sets the size of the output queue for each worker.
		
		// register the basic set Kryo serializers
		registerSerialization(Frame.class, FrameSerializer.class);
		registerSerialization(Descriptor.class, DescriptorSerializer.class);
		registerSerialization(Feature.class, FeatureSerializer.class);
		
		// register FileAdaptors
		ArrayList<String> adaptorList = new ArrayList<String>();
		adaptorList.add(LocalFileAdaptor.class.getName());
		adaptorList.add(S3Adaptor.class.getName());
		adaptorList.add(ClasspathAdaptor.class.getName());
		adaptorList.add(FtpAdaptor.class.getName());
		put(StormCVConfig.STORMCV_ADAPTORS, adaptorList);
	}
	
	/**
	 * Registers an adaptorclass which can be used throughout the topology
	 * @param adaptorClass
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public StormCVConfig registerAdaptor(Class<? extends FileAdaptor> adaptorClass){
		((ArrayList<String>)get(StormCVConfig.STORMCV_ADAPTORS)).add(adaptorClass.getName());
		return this;
	}
	
}
