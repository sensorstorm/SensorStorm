package nl.tno.sensortimeseries.operation;

import java.util.List;

import nl.tno.sensortimeseries.model.Measurement;

public interface SingleTimerTaskÌnterface {
	
	public List<Measurement> doTimerSingleTask(long timestamp);

}
