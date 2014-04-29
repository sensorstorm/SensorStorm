package nl.tno.sensortimeseries.operation;

public interface TimerControllerInterface {

	public void registerSensorForRecurringTimerTask(String sensorid, long timerFreq, RecurringTimerTaskÌnterface recurringTimerTaskÌnterface);
	
	public void registerSensorForSingleTimerTask(String sensorid, long sleepTime, SingleTimerTaskÌnterface singleTimerTaskÌnterface);

}
