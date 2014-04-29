package nl.tno.sensortimeseries.operation;

import java.util.List;

import nl.tno.sensortimeseries.model.Measurement;

public interface RecurringTimerTaskÌnterface {
	
	public List<Measurement> doTimerRecurringTask(long timestamp);

}
