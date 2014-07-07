package nl.tno.timeseries.timer;

import java.util.List;

import nl.tno.timeseries.interfaces.Particle;

public interface TimerTaskInterface {

	public void setTimerController(TimerControllerÌnterface timerController);
	
	public List<Particle> doTimerRecurringTask(long timestamp);

	public List<Particle> doTimerSingleTask(long timestamp);

}
