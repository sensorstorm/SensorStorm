package nl.tno.stormcv.batcher;

import java.util.List;
import java.util.Map;

import nl.tno.storm.configuration.api.ZookeeperStormConfigurationAPI;
import nl.tno.timeseries.channels.ParticleCache;
import nl.tno.timeseries.interfaces.Batcher;
import nl.tno.timeseries.interfaces.BatcherException;
import nl.tno.timeseries.interfaces.DataParticle;
import nl.tno.timeseries.interfaces.DataParticleBatch;

public class SlidingWindowBatcher implements Batcher{

	private int windowSize;
	private int sequenceDelta;
	private int maxSize = Integer.MAX_VALUE;
	
	public SlidingWindowBatcher(int windowSize, int sequenceDelta){
		this.windowSize = windowSize;
		this.sequenceDelta = sequenceDelta;
	}
	
	public SlidingWindowBatcher maxSize(int size){
		this.maxSize = size;
		return this;
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
	public void init(String channelID, long startTimestamp,
			Map stormNativeConfig,
			ZookeeperStormConfigurationAPI zookeeperStormConfiguration)
			throws BatcherException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public List<DataParticleBatch> batch(ParticleCache cache,
			DataParticle inputParticle) throws BatcherException {
		// TODO Auto-generated method stub
		return null;
	}

}
