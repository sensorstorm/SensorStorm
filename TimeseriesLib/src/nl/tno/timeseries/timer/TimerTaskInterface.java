package nl.tno.timeseries.timer;

import java.util.List;

import nl.tno.timeseries.interfaces.DataParticle;

public interface TimerTaskInterface {

	public void setTimerController(TimerControllerInterface timerController);

	public List<DataParticle> doTimerRecurringTask(long timestamp);

	public List<DataParticle> doTimerSingleTask(long timestamp);

}
