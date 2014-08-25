package nl.tno.timeseries.timer;

/**
 * This interface describes how an TimerTask itself can register for single
 * and/or recurring timer tasks. Each registration also needs a channelId to
 * specify to which channel particles this timer must sync itself.
 * 
 * @author waaijbdvd
 * 
 */
public interface TimerControllerInterface {

	/**
	 * Register for a recurring task.
	 * 
	 * @param channelId
	 *            The id of the channel to which the timer must be synced to.
	 * @param timerFreq
	 *            The frequency how often the recurring task must be triggerd.
	 * @param timerTask
	 *            A link to the timerTask recurring method to be called.
	 */
	public void registerOperationForRecurringTimerTask(String channelId,
			long timerFreq, TimerTaskInterface timerTask);

	/**
	 * Register for a single task.
	 * 
	 * @param channelId
	 *            The id of the channel to which the timer must be synced to.
	 * @param timerFreq
	 *            The frequency how often the single task must be triggerd.
	 * @param timerTask
	 *            A link to the timerTask single method to be called.
	 */
	public void registerOperationForSingleTimerTask(String channelId,
			long wakeupTime, TimerTaskInterface timerTask);

}
