package nl.tno.timeseries.timer;



public interface TimerControllerÌnterface {

	public void registerOperationForRecurringTimerTask(String channelId, long timerFreq, TimerTaskInterface timerTask);
	
	public void registerOperationForSingleTimerTask(String channelId, long wakeupTime, TimerTaskInterface timerTask);


}
