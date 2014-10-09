package nl.tno.stormcv.operation;

import java.awt.Rectangle;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.opencv.core.Mat;
import org.opencv.core.MatOfByte;
import org.opencv.core.MatOfRect;
import org.opencv.core.Rect;
import org.opencv.core.Size;
import org.opencv.highgui.Highgui;
import org.opencv.objdetect.CascadeClassifier;

import backtype.storm.utils.Utils;
import nl.tno.storm.configuration.api.ZookeeperStormConfigurationAPI;
import nl.tno.timeseries.annotation.OperationDeclaration;
import nl.tno.timeseries.interfaces.DataParticle;
import nl.tno.timeseries.interfaces.OperationException;
import nl.tno.timeseries.interfaces.SingleOperation;
import nl.tno.stormcv.particle.Descriptor;
import nl.tno.stormcv.particle.Frame;
import nl.tno.stormcv.particle.Feature;
import nl.tno.stormcv.util.NativeUtils;

/**
 * Operation that uses OpenCV's Cascade Classifier for object detection. The object model (haar features)
 * used must be present on the classpath. The name of the objects detected as well as the model must be provided on construction.
 * Other (optional) parameters used by the classifier can be set using the setters.
 * 
 * This operation can provide two different kind of outputs (configured through the outputFrame method):
 * <ul>
 * <li>{@link Feature} object containing all the detected objects as {@link Descriptor}. I.E. the frame will be lost after this operation.</li>
 * <li>The received {@link Frame} containing the {@link Feature} as described above. I.E. the frame will be sent to the next bolt together with the detected objects</li>
 * </ul>
 * 
 * @author Corne Versloot
 *
 */
@OperationDeclaration(inputs = { Frame.class}, outputs = {Frame.class, Feature.class})
public class CascadeClassifierOperation extends OpenCVOperation implements SingleOperation {

	private static final long serialVersionUID = 1864850034278302075L;
	private String name;
	private String model;
	private int[] minSize = new int[]{0,0};
	private int[] maxSize = new int[]{1000, 1000};
	private int minNeighbors = 3;
	private int flags = 0;
	private float scaleFactor = 1.1f;
	private boolean outputFrame = false;
	
	private CascadeClassifier haarDetector;

	/**
	 * Creates a CascasdeClassifier using the provided model. Features created by this operation will get the featureName as their name value.
	 * @param featureName
	 * @param modelXML
	 */
	public CascadeClassifierOperation(String featureName, String modelXML) {
		this.name = featureName;
		this.model = modelXML;
	}

	public CascadeClassifierOperation minSize(int w, int h){
		this.minSize = new int[]{w, h};
		return this;
	}
	
	public CascadeClassifierOperation maxSize(int w, int h){
		this.maxSize = new int[]{w, h};
		return this;
	}
	
	public CascadeClassifierOperation minNeighbors(int number){
		this.minNeighbors = number;
		return this;
	}
	
	public CascadeClassifierOperation flags(int f){
		this.flags = f;
		return this;
	}
	
	public CascadeClassifierOperation scale(float factor){
		this.scaleFactor  = factor;
		return this;
	}
	
	/**
	 * Sets the output of this Operation to be a {@link Frame} which contains all the features. If set to false
	 * this Operation will return each {@link Feature} separately. Default value after construction is FALSE
	 * @param frame
	 * @return
	 */
	public CascadeClassifierOperation outputFrame(boolean frame){
		this.outputFrame = frame;
		return this;
	}
	
	/**
	 * Loads OpenCV and the Haar features provided on construction.
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public void init(String channelId, long sequenceNr, Map stormConf, ZookeeperStormConfigurationAPI zookeperConf) throws OperationException{
		Object libName = stormConf.get(OpenCVOperation.LIBNAME_CONFIG_KEY);
		if(getLibName() == null && libName != null){
			this.libName((String)libName);
		}
		
		try{
			loadOpenCV();
			if(model.charAt(0) != '/') model = "/"+model;
			File cascadeFile = NativeUtils.extractTmpFileFromJar(model, true);
			haarDetector = new CascadeClassifier(cascadeFile.getAbsolutePath());
			Utils.sleep(100); // make sure the classifier has loaded before removing the tmp xml file
			if(!cascadeFile.delete()) cascadeFile.deleteOnExit();
		}catch(Exception e){
			throw new OperationException(e.getMessage());
		}
	}

	@Override
	public List<DataParticle> execute(DataParticle particle)
			throws OperationException {
		ArrayList<DataParticle> result = new ArrayList<DataParticle>();
		Frame frame = (Frame)particle;
		if(frame.imageType.equals(Frame.NO_IMAGE)) return result;

		MatOfByte mob = new MatOfByte(frame.imageBytes);
		Mat image = Highgui.imdecode(mob, Highgui.CV_LOAD_IMAGE_COLOR);
		
		MatOfRect haarDetections = new MatOfRect();
		haarDetector.detectMultiScale(image, haarDetections, scaleFactor, minNeighbors, flags, new Size(minSize[0], minSize[1]), new Size(maxSize[0], maxSize[1]));
		ArrayList<Descriptor> descriptors = new ArrayList<Descriptor>();
		for(Rect rect : haarDetections.toArray()){
			Rectangle box = new Rectangle(rect.x, rect.y, rect.width, rect.height);
			descriptors.add(new Descriptor(frame.getChannelId(), frame.getTimestamp(), box, 0, new float[0]));
		}
		
		System.err.println("#cars: "+descriptors.size());
		
		Feature feature = new Feature(frame.getChannelId(), frame.getTimestamp(), name, 0, descriptors, null);
		if(outputFrame){
			frame.features.add(feature);
			result.add(frame);
		}else{
			result.add(feature);
		}
		return result;
	}

}
