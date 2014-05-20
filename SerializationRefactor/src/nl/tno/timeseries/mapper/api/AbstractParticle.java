package nl.tno.timeseries.mapper.api;

public abstract class AbstractParticle implements Particle {

	protected String streamId;
	protected long sequenceNr;
	
	@Override
	public String getStreamId() {
		return streamId;
	}

	@Override
	public void setStreamId(String streamId) {
		this.streamId = streamId;
	}

	@Override
	public long getSequenceNr() {
		return sequenceNr;
	}

	@Override
	public void setSequenceNr(long sequenceNr) {
		this.sequenceNr = sequenceNr;
	}
}
