package nl.tno.timeseries.timer;

import java.util.List;

import nl.tno.timeseries.interfaces.DataParticle;

public interface TimerTaskInterface {

	/**
	 * This method is called when the first timerParticle arrives for an
	 * operation.
	 * 
	 * @param timerController
	 */
	public void setTimerController(TimerControllerInterface timerController);

	public List<DataParticle> doTimerRecurringTask(long timestamp);

	public List<DataParticle> doTimerSingleTask(long timestamp);

}
