package nl.tno.stormcv.operation;

import java.awt.Rectangle;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.opencv.core.Mat;
import org.opencv.core.MatOfByte;
import org.opencv.core.MatOfKeyPoint;
import org.opencv.features2d.DescriptorExtractor;
import org.opencv.features2d.FeatureDetector;
import org.opencv.features2d.KeyPoint;
import org.opencv.highgui.Highgui;

import nl.tno.storm.configuration.api.ZookeeperStormConfigurationAPI;
import nl.tno.stormcv.particle.Descriptor;
import nl.tno.stormcv.particle.Feature;
import nl.tno.stormcv.particle.Frame;
import nl.tno.timeseries.annotation.OperationDeclaration;
import nl.tno.timeseries.interfaces.DataParticle;
import nl.tno.timeseries.interfaces.OperationException;
import nl.tno.timeseries.interfaces.SingleOperation;

/**
 * An operation used to detect and describe a wide variety of features using the OpenCV FeatureExtraction and 
 * DescriptorExtractor functions. The name, detector type and extractor type must be provided upon construction.
 * Operation on a single frame results in a single Feature instance containing a (possibly empty) set of
 * descriptors. Descriptor length depends on the descriptor type used.
 * This operation either returns the created {@link Feature} object (default) or the {@link Frame} it received containing this feature. 
 * 
 * @author Corne Versloot
 *
 */
@OperationDeclaration(inputs = { Frame.class}, outputs = {Frame.class, Feature.class})
public class FeatureExtractionOperation extends OpenCVOperation implements
		SingleOperation {

	private static final long serialVersionUID = -7934584728216926535L;
	private int detectorType;
	private int descriptorType;
	private String featureName;
	private boolean outputFrame = false;

	/**
	 * @param featureName the name of the feature (i.e. SIFT, SURF, ...) which will be put in the generated Feature's name field
	 * @param detectorType the keypoint detection algorithm to use, must be one of org.opencv.features2d.FeatureDetector constants 
	 * @param descriptorType the type of descriptor to use, must be one of <a href=org.opencv.features2d.DescriptorExtractor constants
	 * @see <a href="http://docs.opencv.org/java/index.html?org/opencv/features2d/FeatureDetector.html">OpenCV FeatureDetector</a>
	 * @see <a href="http://docs.opencv.org/java/index.html?org/opencv/features2d/FeatureDetector.html">OpenCV DescriptorExtractor</a>
	 */
	public FeatureExtractionOperation(String featureName, int detectorType, int descriptorType){
		this.featureName = featureName;
		this.detectorType = detectorType;
		this.descriptorType = descriptorType;
	}

	/**
	 * Sets the output of this Operation to be a {@link Frame} which contains all the features. If set to false
	 * this Operation will return each {@link Feature} separately. Default value after construction is FALSE
	 * @param frame
	 * @return
	 */
	public FeatureExtractionOperation outputFrame(boolean frame){
		this.outputFrame = frame;
		return this;
	}
	
	/**
	 * Loads OpenCV.
	 */
	@Override
	@SuppressWarnings("rawtypes")
	public void init(String channelId, long sequenceNr, Map stormConf, ZookeeperStormConfigurationAPI zookeperConf) throws OperationException{
		Object libName = stormConf.get(OpenCVOperation.LIBNAME_CONFIG_KEY);
		if(getLibName() == null && libName != null){
			this.libName((String)libName);
		}
		try {
			loadOpenCV();
		} catch (RuntimeException | IOException e) {
			throw new OperationException("Unable to instantiate ColorHistogramOperation due to: "+e.getMessage(), e);
		}		
	}

	@Override
	public List<DataParticle> execute(DataParticle particle) throws OperationException {
		ArrayList<DataParticle> result = new ArrayList<DataParticle>();
		Frame frame = (Frame)particle;
		
		if(frame.imageType.equals(Frame.NO_IMAGE)) return result;
		MatOfByte mob = new MatOfByte(frame.imageBytes);
		Mat image = Highgui.imdecode(mob, Highgui.CV_LOAD_IMAGE_ANYCOLOR);
		
		FeatureDetector siftDetector = FeatureDetector.create(detectorType);
		MatOfKeyPoint mokp = new MatOfKeyPoint();
		siftDetector.detect(image, mokp);
		List<KeyPoint> keypoints = mokp.toList();
		
		Mat descriptors = new Mat();
		DescriptorExtractor extractor = DescriptorExtractor.create(descriptorType);
		extractor.compute(image, mokp, descriptors);
		List<Descriptor> descrList = new ArrayList<Descriptor>();
		float[] tmp = new float[1];
		for(int r=0; r<descriptors.rows(); r++){
			float[] values = new float[descriptors.cols()];
			for(int c=0; c<descriptors.cols(); c++){
				descriptors.get(r, c, tmp);
				values[c] = tmp[0];
			}
			descrList.add(new Descriptor(frame.getChannelId(), frame.getTimestamp(), new Rectangle((int)keypoints.get(r).pt.x, (int)keypoints.get(r).pt.y, 0, 0), 0, values));
		}
		
		Feature feature = new Feature(frame.getChannelId(), frame.getTimestamp(), featureName, 0, descrList, null);
		if(outputFrame){
			frame.features.add(feature);
			result.add(frame);
		}else{
			result.add(feature);
		}
		return result;
	}

}
