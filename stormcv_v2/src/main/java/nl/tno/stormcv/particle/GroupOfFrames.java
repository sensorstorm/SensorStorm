package nl.tno.stormcv.particle;

import java.util.List;

import nl.tno.timeseries.mapper.annotation.TupleField;


public class GroupOfFrames extends CVParticle{

	public static final String FRAMES = "frames";
	
	@TupleField(name = FRAMES)
	public List<Frame> frames;
	
	public GroupOfFrames(){
		super();
	}
	
	public GroupOfFrames(String channelId, long sequenceNr, List<Frame> frames) {
		super(channelId, sequenceNr);
		this.frames = frames;
	}
	
	public int groupSize(){
		if(frames == null) return 0;
		return frames.size();
	}
	
}
