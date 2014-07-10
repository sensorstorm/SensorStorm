package nl.tno.timeseries.interfaces;

import java.io.Serializable;
import java.util.Map;

import backtype.storm.task.TopologyContext;

public interface Fetcher extends Serializable {

	public void prepare(@SuppressWarnings("rawtypes")Map stormConf, TopologyContext context) throws Exception;
	
	public void activate();

	public void deactivate() ;
	
	public DataParticle fetchParticle();
	
}

