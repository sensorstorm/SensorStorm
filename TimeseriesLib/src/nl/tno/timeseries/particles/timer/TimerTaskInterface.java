package nl.tno.timeseries.particles.timer;

import java.util.List;

import nl.tno.timeseries.particles.DataParticle;

/**
 * This interface must be implemented by an object when it wants to receive
 * timerTicks. Via the timerController it can register for single and/or
 * recurring timerTaks events.
 * 
 * @author waaijbdvd
 * 
 */
public interface TimerTaskInterface {

	/**
	 * This method is called when the first timerParticle arrives for an
	 * operation.
	 * 
	 * @param timerController
	 */
	public void setTimerController(TimerControllerInterface timerController);

	/**
	 * this method is called when the recurring timer has produced an event. The
	 * timestamp of the event is passed. The method can return a list containing
	 * zero or more DataParticles that must be send.
	 * 
	 * @param timestamp
	 * @return
	 */
	public List<DataParticle> doTimerRecurringTask(long timestamp);

	/**
	 * this method is called when the single timer has passed, the timestamp is
	 * passed. The method can return a list containing zero or more
	 * DataParticles that must be send.
	 * 
	 * @param timestamp
	 * @return
	 */
	public List<DataParticle> doTimerSingleTask(long timestamp);

}
