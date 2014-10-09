package nl.tno.stormcv.particle;

import java.awt.Rectangle;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import nl.tno.stormcv.util.ImageUtils;
import nl.tno.timeseries.mapper.annotation.TupleField;

/**
 * This {@link CVParticle} implementation represents a full (or partial) frame from a video stream. Frame has the following fields:
 * <ul>
 * <li>channelId: the id of the stream the frame was extracted from</li>
 * <li>sequenceNr: the frame number of the frame in the stream</li>
 * <li>timeStamp: relative timestamp in ms of this frame with respect to the start of the stream (which is 0 ms)</li>
 * <li>image: the actual frame represented as BufferedImage <i>(may be NULL)</i></li>
 * <li>imageBytes: a byte[] representation of image encoded as imageType</li>
 * <li>boundingBox: indicates which part of the original frame this object represents. This might be the full frame but can also be 
 * some other region of interest. This box can be used for tessellation or indicate some ROI.</li>
 * <li>features: list with {@link Feature} objects (containing one or more {@link Descriptor}) describing 
 * aspects of the frame such as SIFT, color histogram or Optical FLow (list may be empty)</li>
 * </ul>
 * @author Corne Versloot
 *
 */
public class Frame extends CVParticle{

	public final static String NO_IMAGE = "none";
	public final static String JPG_IMAGE = "jpg";
	public final static String PNG_IMAGE = "png";
	public final static String GIF_IMAGE = "gif";
	
	public final static String TIMESTAMP = "timestamp";
	public final static String IMAGE_TYPE = "image_type";
	public final static String IMAGE_BYTES = "image_bytes";
	public final static String BOUNDING_BOX = "box";
	public final static String FEATURES = "features";
	
	@TupleField(name = TIMESTAMP)
	public long timeStamp;
	
	@TupleField(name = IMAGE_TYPE)
	public String imageType = JPG_IMAGE;
	
	@TupleField(name = IMAGE_BYTES)
	public byte[] imageBytes;
	
	@TupleField(name = BOUNDING_BOX)
	public Rectangle boundingBox;

	@TupleField(name = FEATURES)
	public List<Feature> features = new ArrayList<Feature>();
	
	private BufferedImage image;
	
	public Frame(){
		super();
	}
	
	public Frame(String channelId, long sequenceNr, long timeStamp, String imageType, byte[] imageBytes, Rectangle boundingBox, List<Feature> features) {
		super(channelId, sequenceNr);
		this.timeStamp = timeStamp;
		this.imageType = imageType;
		this.imageBytes = imageBytes;
		this.boundingBox = boundingBox;
		if(features != null) this.features = features;
	}
	
	public Frame(String channelId, long sequenceNr, long timeStamp, String imageType, BufferedImage image, Rectangle boundingBox, List<Feature> features) throws IOException {
		super(channelId, sequenceNr);
		this.timeStamp = timeStamp;
		this.imageType = imageType;
		setImage(image);
		this.boundingBox = boundingBox;
		if(features != null) this.features = features;
	}
	
	public BufferedImage getImage() throws IOException {
		if(imageBytes == null) {
			imageType = NO_IMAGE;
			return null;
		}
		if(image == null){
			image = ImageUtils.bytesToImage(imageBytes);
		}
		return image;
	}

	public void setImage(BufferedImage image) throws IOException {
		this.image = image;
		if(image != null){
			if(imageType.equals(NO_IMAGE)) imageType = JPG_IMAGE;
			this.imageBytes = ImageUtils.imageToBytes(image, imageType);
		}else{
			this.imageBytes = null;
			this.imageType = NO_IMAGE;
		}
	}
	
	public void setImage(byte[] imageBytes, String imgType){
		this.imageBytes = imageBytes;
		this.imageType = imgType;
		this.image = null;
	}
	
	public void removeImage(){
		this.image = null;
		this.imageBytes = null;
		this.imageType = NO_IMAGE;
	}
	
	public void setImageType(String imageType) throws IOException {
		this.imageType = imageType;
		if(image != null){
			imageBytes = ImageUtils.imageToBytes(image, imageType);
		}else{
			image = ImageUtils.bytesToImage(imageBytes);
			imageBytes = ImageUtils.imageToBytes(image, imageType);
		}
	}
	
}
