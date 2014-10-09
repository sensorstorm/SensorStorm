package nl.tno.stormcv.particle;

import java.util.HashMap;

import nl.tno.timeseries.interfaces.DataParticle;
import nl.tno.timeseries.mapper.annotation.TupleField;

/**
 * An abstract Computer Vision Particle superclass which contains the information required
 * for all objects processed by the platform. Most of the standard components
 * within StormCV make use of these two fields to group, filter and order objects they get. 
 *  
 * @author Corne Versloot
 *
 */
public abstract class CVParticle implements DataParticle{

	public static final String CHANNEL_ID = "channel_id";
	public static final String TIMESTAMP = "timestamp";
	public static final String METADATA = "metadata";
	
	@TupleField(name = CHANNEL_ID)
	public String channelId;
	
	@TupleField(name = TIMESTAMP)
	public long timestamp;
	
	@TupleField(name = METADATA)
	public HashMap<String, Object> metadata = new HashMap<String, Object>();
	
	public CVParticle(){
		this.channelId = "channel_not_set";
		this.timestamp = 0;
	}
	
	public CVParticle(String channelId, long sequenceNr) {
		this.channelId = channelId;
		this.timestamp = sequenceNr;
	}

	@Override
	public String getChannelId() {
		return channelId;
	}

	@Override
	public void setChannelId(String channelId) {
		this.channelId = channelId;
	}

	@Override
	public long getTimestamp() {
		return timestamp;
	}

	@Override
	public void setTimestamp(long timeStamp) {
		this.timestamp = timeStamp;		
	}
	
	@Override
	public int hashCode(){
		return (channelId+timestamp).hashCode();
	}
	
	@Override
	public boolean equals(Object object){
		if(object instanceof CVParticle){
			CVParticle cvp2 = (CVParticle)object;
			return cvp2.getChannelId().equals(channelId) && cvp2.getTimestamp() == timestamp;
		}
		return false;
	}
}
