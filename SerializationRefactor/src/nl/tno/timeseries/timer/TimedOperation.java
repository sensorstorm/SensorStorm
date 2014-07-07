package nl.tno.timeseries.timer;

import java.util.Map;

import nl.tno.timeseries.interfaces.Operation;

public interface TimedOperation extends Operation, TimerTaskInterface {
	
	public void init(String channelID, long startTimestamp, @SuppressWarnings("rawtypes")Map stromConfig);

}
