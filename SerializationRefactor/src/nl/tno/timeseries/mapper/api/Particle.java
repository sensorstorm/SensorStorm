package nl.tno.timeseries.mapper.api;

public interface Particle {

	public String getStreamId();
	public void setStreamId(String streamId);

	public long getSequenceNr();
	public void setSequenceNr(long sequenceNr);

}
