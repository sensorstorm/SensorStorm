package nl.tno.sensortimeseries.operation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import nl.tno.sensortimeseries.algorithms.BaseSensorAlgorithm;
import nl.tno.sensortimeseries.model.Measurement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SensorAlgorithmOperation extends SensorOperation implements TimerControllerInterface {
	private static final long serialVersionUID = -3388252897963148969L;
	private Logger logger = LoggerFactory.getLogger(SensorAlgorithmOperation.class);
	private @SuppressWarnings("rawtypes") Map stormConfig;
	private Map<String, BaseSensorAlgorithm> algorithms;
	protected Class<? extends BaseSensorAlgorithm> algorithmClass;
	private Map<String, RecurringTask> recurringAlgorithmTasks;
	private Map<String, SingleTask> singleAlgorithmTasks;

	
	public SensorAlgorithmOperation(Class<? extends BaseSensorAlgorithm> algorithmClass) {
		this.algorithmClass = algorithmClass;
	}
	
	
	protected void initSensorOperation(@SuppressWarnings("rawtypes") Map conf) {
		this.stormConfig = conf;
		algorithms = new HashMap<String, BaseSensorAlgorithm>();
		
		// register for all timerticks
		recurringAlgorithmTasks = new HashMap<String, RecurringTask>();
		singleAlgorithmTasks = new HashMap<String, SingleTask>();
		resetRecurringToMainTimerfreq();
	} 

	
	@Override
	protected List<Measurement> processMeasurement(Measurement measurement) {
		System.out.println("SensorAlgorithmOperation.processMeasurement("+measurement+")");
		BaseSensorAlgorithm algorithm = algorithms.get(measurement.getSensorid());
		List<Measurement> result = null;
		try { 
			if (algorithm == null) {
				algorithm = algorithmClass.newInstance();
				algorithm.initAlgorithm(measurement.getSensorid(), measurement.getTimestamp(), stormConfig, this);
				algorithms.put(measurement.getSensorid(), algorithm);
			} 
			result = algorithm.handleMeasurement(measurement);
		} catch (InstantiationException | IllegalAccessException | NullPointerException e) {
			logger.error("Error can not instantiate Algorithm: "+e.getMessage());
		}
		
		return result;
	}
	
	

	public void registerSensorForRecurringTimerTask(String sensorid, long timerFreq, RecurringTimerTaskÌnterface recurringTimerTaskÌnterface) {
		RecurringTask recurringTask = recurringAlgorithmTasks.get(sensorid);
		if (recurringTask == null) {
			recurringTask = new RecurringTask(timerFreq, 0, recurringTimerTaskÌnterface);
			recurringAlgorithmTasks.put(sensorid, recurringTask);
		} else {
			recurringTask.timerFreq = timerFreq;
			recurringTask.lastTimestamp = 0;
		}
	}

	
	public void registerSensorForSingleTimerTask(String sensorid, long wakeupTime, SingleTimerTaskÌnterface singleTimerTaskÌnterface) {
		SingleTask singleTask = singleAlgorithmTasks.get(sensorid);
		if (singleTask == null) {
			singleTask = new SingleTask(wakeupTime, singleTimerTaskÌnterface);
			singleAlgorithmTasks.put(sensorid, singleTask);
		} else {
			singleTask.wakeupTime = wakeupTime;
		}
	}

	
	@Override
	protected List<Measurement> doTimerSingleTask(long timeStamp) {
		return null;
	}
	
	protected List<Measurement> doTimerRecurringTask(long timestamp) {
		ArrayList<Measurement> result = new ArrayList<Measurement>();

		// check if there are pending recurring sensor tasks
		for (String sensorid : recurringAlgorithmTasks.keySet()) {
			RecurringTask recurringTask = recurringAlgorithmTasks.get(sensorid);
			if (recurringTask.timerFreq != 0) {
				if (recurringTask.lastTimestamp == 0) {
					recurringTask.lastTimestamp = timestamp;
				}
				while (timestamp - recurringTask.lastTimestamp >= recurringTask.timerFreq) {
					recurringTask.lastTimestamp = recurringTask.lastTimestamp + recurringTask.timerFreq;
					List<Measurement> recurringMeasurements = recurringTask.recurringTimerTaskHandler.doTimerRecurringTask(recurringTask.lastTimestamp);
					if (recurringMeasurements != null) {
						result.addAll(recurringMeasurements);
					}
				}
			}
		}
		
		// check if there are pending single sensor tasks
		for (String sensorid : singleAlgorithmTasks.keySet()) {
			SingleTask singleTask = singleAlgorithmTasks.get(sensorid);
			if (singleTask.wakeupTime != 0) {
				while ((singleTask.wakeupTime != 0) && (timestamp >= singleTask.wakeupTime)) {
					// eerst het element uit de lijst voordat de task wordt uitgevoerd 
					// omdat die task wel eens weer voor dezelfde sensor een timertask wil toevoegen
					long timerTaskTimestamp = singleTask.wakeupTime;
					singleTask.wakeupTime = 0;
					List<Measurement> singleMeasurements = singleTask.singleTimerTaskHandler.doTimerSingleTask(timerTaskTimestamp);
					if (singleMeasurements != null) {
						result.addAll(singleMeasurements);
					}
				}
			}
		}
		
		if (result.size() > 0) {
			return result;
		} else {
			return null;
		}
	}
	
	
	
	class RecurringTask {
		public long timerFreq;
		public long lastTimestamp;
		public RecurringTimerTaskÌnterface recurringTimerTaskHandler;
		
		public RecurringTask(long recurringTimerFreq, long lastRecurringTimestamp, RecurringTimerTaskÌnterface recurringTimerTaskÌnterface) {
			this.timerFreq = recurringTimerFreq;
			this.lastTimestamp = lastRecurringTimestamp;
			this.recurringTimerTaskHandler = recurringTimerTaskÌnterface;
		}
	}
	
	class SingleTask {
		public long wakeupTime;
		public SingleTimerTaskÌnterface singleTimerTaskHandler;
		
		public SingleTask(long sleepTimeSingleWakeup, SingleTimerTaskÌnterface singleTimerTaskÌnterface) {
			this.wakeupTime = sleepTimeSingleWakeup;
			this.singleTimerTaskHandler = singleTimerTaskÌnterface;
		}
	}

}
