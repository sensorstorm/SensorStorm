package nl.tno.stormcv.operation;

import java.awt.Rectangle;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.opencv.core.Mat;
import org.opencv.core.MatOfByte;
import org.opencv.core.MatOfFloat;
import org.opencv.core.MatOfInt;
import org.opencv.highgui.Highgui;
import org.opencv.imgproc.Imgproc;

import nl.tno.storm.configuration.api.ZookeeperStormConfigurationAPI;
import nl.tno.stormcv.particle.Descriptor;
import nl.tno.stormcv.particle.Feature;
import nl.tno.stormcv.particle.Frame;
import nl.tno.timeseries.annotation.OperationDeclaration;
import nl.tno.timeseries.interfaces.DataParticle;
import nl.tno.timeseries.interfaces.OperationException;
import nl.tno.timeseries.interfaces.SingleOperation;

/**
 * A {@link SingleOperation} that calculates the color histogram for frames it receives using OpenCV's 
 * calcHist function. The configuration to be used can be set using the configure function. This operation either
 * returns the created {@link Feature} object or the {@link Frame} it received containing this feature.  
 *  
 * @author Corne Versloot
 *
 */
@OperationDeclaration(inputs = { Frame.class}, outputs = {Frame.class, Feature.class})
public class ColorHistogramOperation extends OpenCVOperation implements SingleOperation {

	private static final long serialVersionUID = 5999109593304409469L;
	private String name;
	private int[] chansj = new int[]{0,1,2};
	private int[] histsizej = new int[]{255, 255, 255};
	private float[] rangesj = new float[]{0, 256, 0, 256, 0, 256 };
	private boolean outputFrame = false;
	
	/**
	 * Creates a {@link ColorHistogramOperation} that will produce {@link Feature} objects with the configured
	 * name as the feature's name.
	 * @param name
	 */
	public ColorHistogramOperation(String name){
        this.name = name;
	}

	/**
	 * Configure the HistorgramOperation. The default is set for use of RGB images
	 * @param chans list with channal id's default = {0, 1, 2}
	 * @param histsize for each channel the number of bins to use, default = {255, 255 ,255}
	 * @param ranges for each channel the min. and max. values present, default = {0, 256, 0, 256, 0, 256 }
	 * @return
	 */
	public ColorHistogramOperation configure(int[] chans, int[] histsize, float[] ranges){
        chansj = chans;
        histsizej = histsize;
        rangesj = ranges;
        return this;
	}
	
	/**
	 * Sets the output of this Operation to be a {@link Frame} which contains all the features. If set to false
	 * this Operation will return each {@link Feature} separately. Default value after construction is FALSE
	 * @param frame
	 * @return
	 */
	public ColorHistogramOperation outputFrame(boolean frame){
		this.outputFrame = frame;
		return this;
	}
	
	/**
	 * Loads OpenCV.
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public void init(String channelId, long sequenceNr, Map stormConf, ZookeeperStormConfigurationAPI zookeperConf) throws OperationException{
		Object libName = stormConf.get(OpenCVOperation.LIBNAME_CONFIG_KEY);
		if(getLibName() == null && libName != null){
			this.libName((String)libName);
		}
		try {
			loadOpenCV();
		} catch (RuntimeException | IOException e) {
			throw new OperationException("Unable to instantiate ColorHistogramOperation due to: "+e.getMessage());
		}		
	}

	@Override
	public List<DataParticle> execute(DataParticle particle) throws OperationException {
		ArrayList<DataParticle> result = new ArrayList<DataParticle>();
		Frame frame = (Frame)particle;
		
		MatOfByte mob = new MatOfByte(frame.imageBytes);
		Mat image = Highgui.imdecode(mob, Highgui.CV_LOAD_IMAGE_COLOR);
		Mat hist = new Mat();

        MatOfInt chans;
        MatOfInt histsize;
        MatOfFloat ranges;

        List<Mat> images = new ArrayList<Mat>();

		ArrayList<Descriptor> hist_descriptors      = new ArrayList<Descriptor>();
		
        Rectangle box = new Rectangle(0, 0, (int) image.size().width, (int) image.size().height); // size of image get boundingbox from sf

        images.add(image);
        for (int i = 0; i < chansj.length; i++){
            chans = new MatOfInt(chansj[i]);
            histsize = new MatOfInt(histsizej[i]);
            ranges = new MatOfFloat(rangesj[i*2],rangesj[i*2+1]);
            Imgproc.calcHist(images, chans, new Mat(), hist, histsize, ranges);

            float[] tmp = new float[1];

            int rows = (int) hist.size().height;
            
            float[] values = new float[rows];
            int c = 0;
            for (int r = 0; r < rows; r++) // loop over rows/columns
            {
                hist.get(r, c, tmp);
                values[r] = tmp[0];
            }
            hist_descriptors.add(new Descriptor(frame.getChannelId(), frame.getTimestamp(), box, 0, values));
        }
		
        Feature feature = new Feature( frame.getChannelId(), frame.getTimestamp(), name, 0, hist_descriptors, null );
        
        if(outputFrame){
			frame.features.add(feature);
			result.add(frame);
		}else{
			result.add(feature);
		}
		return result;
	}

}
