package nl.tno.timeseries.operation;

import java.io.Serializable;
import java.util.Map;

import nl.tno.timeseries.model.Streamable;
import nl.tno.timeseries.serializer.StreamableSerializer;

import backtype.storm.task.TopologyContext;

public interface Operation <Output extends Streamable> extends Serializable{

	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context) throws Exception;
	
	public void deactivate();
	
	public StreamableSerializer<Output> getSerializer();
	
}
