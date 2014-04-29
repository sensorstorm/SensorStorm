package nl.tno.sensortimeseries.operation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import nl.tno.sensortimeseries.model.Measurement;
import nl.tno.sensortimeseries.model.TimerTick;
import nl.tno.sensortimeseries.serializer.MeasurementSerializer;
import nl.tno.sensortimeseries.utils.TSSensorConfig;
import nl.tno.timeseries.model.Streamable;
import nl.tno.timeseries.operation.SingleInputOperation;
import nl.tno.timeseries.serializer.StreamableSerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.TopologyContext;

abstract public class SensorOperation implements SingleInputOperation<Measurement> {
	private static final long serialVersionUID = 4376070006695965872L;

	private static final Streamable Measurement = null;
	
	private Logger logger = LoggerFactory.getLogger(SensorOperation.class);
	private StreamableSerializer serializer = new MeasurementSerializer();

	private Long mainTimerTickFreq;
	private long recurringTimerFreq = 0;
	private long lastRecurringTimestamp = 0;
	private long sleepTimeSingleWakeup = 0;
	private long lastSingleTimestamp = 0;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context) throws Exception {
		mainTimerTickFreq = conf.get(TSSensorConfig.BASESENSOR_SPOUT_MAIN_TIMERTICK_FREQ) == null ? 0 : (Long)conf.get(TSSensorConfig.BASESENSOR_SPOUT_MAIN_TIMERTICK_FREQ);
		initSensorOperation(conf);
	}
	
	/**
	 * Place to put init functionality
	 */
	abstract protected void initSensorOperation(@SuppressWarnings("rawtypes") Map conf);

	abstract protected List<Measurement> processMeasurement(Measurement measurement);

	abstract protected List<Measurement> doTimerSingleTask(long timeStamp);

	abstract protected List<Measurement> doTimerRecurringTask(long timeStamp);
	
	
	
	@Override
	public void deactivate() {
	}

	@Override
	public StreamableSerializer<Measurement> getSerializer() {
		return serializer;
	}
	
	
	@Override
	public List<Measurement> execute(Streamable streamable) throws Exception {
		ArrayList<Measurement> result = new ArrayList<Measurement>();
		if (streamable == null) {
			return result;
		}

		if (streamable instanceof Measurement) {
			Measurement measurement = (Measurement)streamable;
			if (measurement.getValueClass().isAssignableFrom(TimerTick.class)) {
				processTimerTick(measurement, result);
			} else {
				List<Measurement> measurements = processMeasurement(measurement);
				if (measurements != null) {
					result.addAll(measurements);
				}
			}
		} else {
			logger.error("can not process measurement of type "+streamable.getClass().getSimpleName()+" only TimerTicks and Measurement<?> can be processed");
		}
		
		return result;
	}
	
	
	private void processTimerTick(Measurement timerTick, List<Measurement> result) {
		if (recurringTimerFreq != 0) {
			if (lastRecurringTimestamp == 0) {
				lastRecurringTimestamp = timerTick.getTimestamp();
			}
			while (timerTick.getTimestamp() - lastRecurringTimestamp >= recurringTimerFreq) {
				lastRecurringTimestamp = lastRecurringTimestamp + recurringTimerFreq;
				List<Measurement> recurringMeasurements = doTimerRecurringTask(lastRecurringTimestamp);
				if (recurringMeasurements != null) {
					result.addAll(recurringMeasurements);
				}
			}
		}
		if (sleepTimeSingleWakeup != 0) {
			if (lastSingleTimestamp == 0) {
				lastSingleTimestamp = timerTick.getTimestamp();
			}
			if (timerTick.getTimestamp() - lastSingleTimestamp >= sleepTimeSingleWakeup) {
				List<Measurement> singleMeasurements = doTimerSingleTask(lastSingleTimestamp + sleepTimeSingleWakeup);
				if (singleMeasurements != null) {
					result.addAll(singleMeasurements);
				}
				sleepTimeSingleWakeup = 0;
			}
		}
		
		// pass timertick on
		result.add(timerTick);
	}

	
	protected void setRecurringTimerFreq(long timerFreq) {
		if (timerFreq < mainTimerTickFreq) {
			logger.warn("Freq ("+timerFreq+") smaller than main timer freq ("+mainTimerTickFreq+"). This can lead to too late timerTasls");
		}
		this.recurringTimerFreq = timerFreq;
	}
	
	
	protected void resetRecurringToMainTimerfreq() {
		this.recurringTimerFreq = mainTimerTickFreq;
	}
	

	public void setSleepTimeSingleTask(long now, long sleepTime) {
		this.sleepTimeSingleWakeup = sleepTime;
	}
	

}
