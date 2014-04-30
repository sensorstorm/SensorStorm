package nl.tno.timeseries.model;

import java.util.HashMap;

import nl.tno.timeseries.serializer.StreamableSerializer;


import backtype.storm.tuple.Tuple;

/**
 * Abstract class representing GenericType superclass which contains the information required
 * (streamId, sequenceNumber) for all types processed by the platform. Most of the standard components
 * within stormCV make use of these two fields to group, filter and order information they get. 
 * 
 * @author Corne Versloot
 *
 */
public abstract class Streamable implements Comparable<Streamable>{
	
	private Tuple tuple;
	private String streamId;
	private long sequenceNr;
	private HashMap<String, Object> metadata = new HashMap<String, Object>();
	
	/**
	 * Constructs a generic type based on the provided tuple. The tuple must contain streamID and sequenceNR
	 * values. 
	 * @param tuple
	 */
	@SuppressWarnings("unchecked")
	public Streamable(Tuple tuple){
		this(tuple.getStringByField(StreamableSerializer.STREAMID), tuple.getLongByField(StreamableSerializer.SEQUENCENR));
		this.tuple = tuple;
		this.setMetadata((HashMap<String, Object>)tuple.getValueByField(StreamableSerializer.METADATA));
	}
	
	/**
	 * Constructs a GenericType object for some piece of information regarding a stream
	 * @param streamId the id of the stream
	 * @param sequenceNr the sequence number of the the information (used for ordering)
	 */
	public Streamable(String streamId, long sequenceNr){
		this.streamId = streamId;
		this.sequenceNr = sequenceNr;
	}
	
	public String getStreamId(){
		return streamId;
	}
	
	public long getSequenceNr(){
		return sequenceNr;
	}

	public Tuple getTuple() {
		return tuple;
	}

	public HashMap<String, Object> getMetadata() {
		return metadata;
	}
	
	public void setMetadata(HashMap<String, Object> metadata) {
		if(metadata != null){
			this.metadata = metadata;
		}
	}
	
	/**
	 * Compares one generictype to another based on their sequence number
	 */
//	@Override
	public int compareTo(Streamable other){
		return (int)(getSequenceNr() - other.getSequenceNr());
	}

}
