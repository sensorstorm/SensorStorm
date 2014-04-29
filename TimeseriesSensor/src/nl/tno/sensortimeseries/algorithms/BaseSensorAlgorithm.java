package nl.tno.sensortimeseries.algorithms;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import nl.tno.sensortimeseries.model.Measurement;
import nl.tno.sensortimeseries.operation.RecurringTimerTaskÌnterface;
import nl.tno.sensortimeseries.operation.SingleTimerTaskÌnterface;
import nl.tno.sensortimeseries.operation.TimerControllerInterface;

abstract public class BaseSensorAlgorithm implements Serializable, SingleTimerTaskÌnterface, RecurringTimerTaskÌnterface {
	private static final long serialVersionUID = 3175889757656827653L;
	private TimerControllerInterface timerController;
	protected String incommingSensorid;
	protected long initTimestamp;

	public void initAlgorithm(String incommingSensorid, 
							  long initTimestamp, 
							  @SuppressWarnings("rawtypes") Map conf, 
							  TimerControllerInterface timerController) {
		this.incommingSensorid = incommingSensorid;
		this.initTimestamp = initTimestamp;
		this.timerController = timerController;
	}
	
	abstract public List<Measurement> handleMeasurement(Measurement measurement);

	
	protected void registerSensorForRecurringTimerTask(String sensorid, long timerFreq, RecurringTimerTaskÌnterface recurringTimerTaskÌnterface) {
		timerController.registerSensorForRecurringTimerTask(sensorid, timerFreq, recurringTimerTaskÌnterface);
	}
	
	protected void registerSensorForSingleTimerTask(String sensorid, long sleepTime, SingleTimerTaskÌnterface singleTimerTaskÌnterface) {
		timerController.registerSensorForSingleTimerTask(sensorid, sleepTime, singleTimerTaskÌnterface);
	}


}
