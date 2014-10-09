package nl.tno.stormcv.particle;

import java.awt.Rectangle;

import nl.tno.timeseries.mapper.annotation.TupleField;

/**
 * This {@link CVParticle} implementation represents a sparse descriptor which is part of a {@link Feature} and has the following fields:
 * <ul>
 * <li>boundinbBox: an optionally zero dimensional rectangle indicating the region in the image this Descriptor describes</li>
 * <li>duration: the duration of the descriptor which may apply to temporal features like STIP</li>
 * <li>values: the float array used to store the actual descriptor</li>
 * </ul>  
 * 
 * @author Corne Versloot
 *
 */

public class Descriptor extends CVParticle {

	public static final String BOUNDING_BOX = "bounding_box";
	public static final String DURATION = "duration";
	public static final String VALUES = "values";
	
	@TupleField(name = BOUNDING_BOX)
	public Rectangle boundingBox;
	
	@TupleField(name = DURATION)
	public long duration;
	
	@TupleField(name = VALUES)
	public float[] values;
	
	public Descriptor(){
		super();
	}
	
	public Descriptor(String channelId, long sequenceNr, Rectangle boundingBox, long duration, float[] values) {
		super(channelId, sequenceNr);
		this.channelId = channelId;
		this.timestamp = sequenceNr;
		this.boundingBox = boundingBox;
		this.values = values;
		this.duration = duration;
	}
	
	@Override
	public String getChannelId() {
		return channelId;
	}

	@Override
	public void setChannelId(String channelId) {
		this.channelId = channelId;
	}

}
