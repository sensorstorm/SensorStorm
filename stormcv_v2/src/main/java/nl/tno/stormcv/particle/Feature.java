package nl.tno.stormcv.particle;

import java.util.ArrayList;
import java.util.List;

import nl.tno.timeseries.mapper.annotation.TupleField;

/**
 * This {@link CVParticle} implementation represents a single feature calculated for {@link Frame} and has the following fields:
 * <ul>
 * <li>name: the name of the feature like SIFT, SURF, HOG etc</li>
 * <li>duration: the duration of the feature in case it describes a temporal aspect of multiple frames</li>
 * <li>sparseDescriptors: a list with {@link Descriptor} objects used to described sparse features like SIFT</li>
 * <li>denseDescriptors: a three dimensional float array much like the OpenCV Mat object which can be used to represent 
 * dense features like dense SIFT or Optical Flow</li>
 * </ul>
 * It is not always clear how a specific descriptor should be stored and it is typically up to the characteristics of the 
 * topology and context what is the best way to go.  
 *  
 * @author Corne Versloot
 *
 */
public class Feature extends CVParticle{

	public static final String NAME = "name";
	public static final String DURATION = "duration";
	public static final String SPARSE_DESCR = "sparce";
	public static final String DENSE_DESCR = "dense";

	@TupleField(name = NAME)
	public String name;
	
	@TupleField(name = DURATION)
	public long duration;
	
	@TupleField(name = SPARSE_DESCR)
	public List<Descriptor> sparseDescriptors = new ArrayList<Descriptor>();
	
	@TupleField(name = DENSE_DESCR)
	public float[][][] denseDescriptors = new float[0][0][0];
	
	public Feature(){
		super();
	}
	
	public Feature(String channelId, long sequenceNr, String name, long duration, List<Descriptor> sparseDescriptors, float[][][] denseDescriptors) {
		super(channelId, sequenceNr);
		this.name = name;
		this.duration = duration;
		this.sparseDescriptors = sparseDescriptors;
		this.denseDescriptors = denseDescriptors;
	}

}
