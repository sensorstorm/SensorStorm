package nl.tno.timeseries.test;

import java.util.Map;

import nl.tno.timeseries.mapper.annotation.Mapper;
import nl.tno.timeseries.mapper.api.Particle;

@Mapper(SomeCustomMapper.class)
public class SelfMappedParticle implements Particle {

	public String streamId;
	public long sequenceNr;
	public String id;
	public int intId;
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
