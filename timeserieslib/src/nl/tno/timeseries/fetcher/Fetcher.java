package nl.tno.timeseries.fetcher;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import nl.tno.timeseries.model.Streamable;
import nl.tno.timeseries.serializer.StreamableSerializer;
import backtype.storm.task.TopologyContext;

public interface Fetcher extends Serializable {

	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context) throws Exception;
	
	public StreamableSerializer<Streamable> getSerializer();
	
	public void activate();

	public void deactivate() ;
	
	public List<Streamable> fetchData();
	
}
