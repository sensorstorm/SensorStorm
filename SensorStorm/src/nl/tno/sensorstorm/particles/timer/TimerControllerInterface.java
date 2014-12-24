package nl.tno.sensorstorm.particles.timer;

/**
 * This interface describes how an TimerTask itself can register for single
 * and/or recurring timer tasks.
 * 
 * @author waaijbdvd
 * 
 */
public interface TimerControllerInterface {

	/**
	 * Register for a recurring task.
	 * 
	 * @param timerFreq
	 *            The frequency how often the recurring task must be triggerd in
	 *            milliseconds.
	 * @param timerTask
	 *            A link to the timerTask recurring method to be called.
	 */
	public void registerOperationForRecurringTimerTask(long timerFreqMs,
			TimerTaskInterface timerTask);

	/**
	 * Register for a single task.
	 * 
	 * @param timerFreq
	 *            The frequency how often the single task must be triggerd.
	 * @param timerTask
	 *            A link to the timerTask single method to be called.
	 */
	public void registerOperationForSingleTimerTask(long wakeupTime,
			TimerTaskInterface timerTask);

}
