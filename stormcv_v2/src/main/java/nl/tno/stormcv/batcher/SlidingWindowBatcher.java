package nl.tno.stormcv.batcher;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import nl.tno.storm.configuration.api.ZookeeperStormConfigurationAPI;
import nl.tno.timeseries.interfaces.Batcher;
import nl.tno.timeseries.interfaces.DataParticle;
import nl.tno.timeseries.interfaces.DataParticleBatch;
import nl.tno.timeseries.interfaces.FaultTolerant;

public class SlidingWindowBatcher implements Batcher{

	private int windowSize;
	private int sequenceDelta;
	private int maxSize = Integer.MAX_VALUE;
	private FaultTolerant faultTolerantDelegator;
	
	public SlidingWindowBatcher(int windowSize, int sequenceDelta){
		this.windowSize = windowSize;
		this.sequenceDelta = sequenceDelta;
	}
	
	public SlidingWindowBatcher maxSize(int size){
		this.maxSize = size;
		return this;
	}

	@Override
	public List<DataParticleBatch> batch(DataParticle particle) {
		return null;
	}

	/**
	 * Checks if the provided window fits the required windowSize and sequenceDelta criteria
	 * @param window
	 * @return
	 */
	private boolean assessWindow(List<DataParticle> window){
		if(window.size() != windowSize) return false;
		long previous = window.get(0).getTimestamp();
		for(int i=1; i<window.size(); i++){
			if(window.get(i).getTimestamp() - previous != sequenceDelta) return false;
			previous = window.get(i).getTimestamp();
		}
		return true;
	}

	@Override
	public void init(String channelId, long sequenceNr, Map conf,
			ZookeeperStormConfigurationAPI arg3, FaultTolerant delegator) {
		this.faultTolerantDelegator = delegator;
		
	}

}
