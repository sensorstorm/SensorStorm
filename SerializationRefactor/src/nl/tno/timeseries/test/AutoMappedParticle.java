package nl.tno.timeseries.test;

import java.util.Map;

import nl.tno.timeseries.mapper.annotation.TupleField;
import nl.tno.timeseries.mapper.api.Particle;

public class AutoMappedParticle implements Particle {

	public String streamId;
	public long sequenceNr;

	@TupleField(name = "customNameForId")
	public String id;

	@TupleField
	public int intId;

	@TupleField
	public Map<String, Double> map;

	public int shouldNotBeSaved;

	@Override
	public String getStreamId() {
		return streamId;
	}

	@Override
	public long getSequenceNr() {
		return sequenceNr;
	}

	@Override
	public void setStreamId(String streamId) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setSequenceNr(long sequenceNr) {
		// TODO Auto-generated method stub
		
	}

}
