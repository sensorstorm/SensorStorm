package nl.tno.sensortimeseries.fetcher;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import nl.tno.sensortimeseries.model.Measurement;
import nl.tno.sensortimeseries.model.MeasurementTimerTick;
import nl.tno.sensortimeseries.serializer.MeasurementSerializer;
import nl.tno.sensortimeseries.utils.TSSensorConfig;
import nl.tno.timeseries.fetcher.Fetcher;
import nl.tno.timeseries.model.Streamable;
import nl.tno.timeseries.serializer.StreamableSerializer;
import backtype.storm.task.TopologyContext;

abstract public class SensorFetcher implements Fetcher {
	private static final long serialVersionUID = -8423610211924350631L;

	private StreamableSerializer serializer = new MeasurementSerializer();
	private Long mainTimerTickFreq;
	private Boolean useMeasurementTime;
	private Map<String, Long> lastKnownNows = new HashMap<String, Long>();

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context) throws Exception {
		useMeasurementTime = conf.get(TSSensorConfig.BASESENSOR_SPOUT_USE_MEASUREMENT_TIME) == null ? false : (Boolean)conf.get(TSSensorConfig.BASESENSOR_SPOUT_USE_MEASUREMENT_TIME);
		mainTimerTickFreq = conf.get(TSSensorConfig.BASESENSOR_SPOUT_MAIN_TIMERTICK_FREQ) == null ? 0 : (Long)conf.get(TSSensorConfig.BASESENSOR_SPOUT_MAIN_TIMERTICK_FREQ);
		
		System.out.println("SensorFetcher useMeasurementTime="+useMeasurementTime+" mainTimerTickFreq="+mainTimerTickFreq);
	}

	
	@Override
	public StreamableSerializer<Streamable> getSerializer() {
		return serializer;
	}

	@Override
	public void activate() {
	}

	@Override
	public void deactivate() {
	}

	
	@Override
	public List<Streamable> fetchData() {
		ArrayList<Streamable> result = new ArrayList<Streamable>();
		
		Measurement nextSensorMeasurement = getNextSensorMeasurement();
		// is there a sensor measurement to be emitted?
		if (nextSensorMeasurement != null) {
			Long timestamp = getTimestamp(nextSensorMeasurement);
			emitTimerTicksAndMeasurement(timestamp, nextSensorMeasurement, result);
		} else {	// perhaps liveTime wants to emit a timerTick
			Long timestamp = getTimestamp(null);
			if (timestamp != null) {
				emitTimerTicksAndMeasurement(timestamp, null, result);
			} // no tuple and no timestamp means nothing to emit
		}
		
		return result;
	}
	
	
	/** 
	 * May return null to indicate that there is no measurement to be emitted
	 * @return
	 */
	abstract protected Measurement getNextSensorMeasurement();

	
	private Long getTimestamp(Measurement measurement) {
		if (useMeasurementTime) {
			if (measurement == null) {
				return null;
			} else {
				return measurement.getTimestamp();
			}
		} else { 
			return System.currentTimeMillis();
		}
	}

	
	private void emitTimerTicksAndMeasurement(Long now, Measurement sensorMeasurement, List<Streamable> result) {
		// first check to see if there are timerTicks to be inserted
		emitTimerTicks(now, sensorMeasurement, result);
		
		// then emit optional measurement
		if (sensorMeasurement != null){
			result.add(sensorMeasurement);
		}
	}
	
	
	private void emitTimerTicks(Long now, Measurement sensorMeasurement, List<Streamable> result) {
		if (mainTimerTickFreq != 0) {
			if (sensorMeasurement != null) {
				emitTimerTicksForThisSensor(sensorMeasurement.getSensorid(), now, result);
			} else {
				// no tuple to emit, but still check all timers
				for (String sensorid : lastKnownNows.keySet()) {
					emitTimerTicksForThisSensor(sensorid, now, result);	
				}
			}
		}
	}

	
	private void emitTimerTicksForThisSensor(String sensorid, long now, List<Streamable> result) {
		long lastKnownNow = getLastKnowNow(sensorid, now);
		while (now - lastKnownNow >= mainTimerTickFreq) {
			lastKnownNow = lastKnownNow + mainTimerTickFreq;
			setLastKnowNow(sensorid, lastKnownNow);
			result.add(new MeasurementTimerTick(sensorid, lastKnownNow));
		}
	}

	
	private long getLastKnowNow(String sensorID, long now) {
		Long lastKnowNow = lastKnownNows.get(sensorID);
		if (lastKnowNow == null) {
			setLastKnowNow(sensorID, now);
			return now;
		} else {
			return lastKnowNow;
		}
	}
	
	
	private void setLastKnowNow(String sensorID, long lastKnownNow) {
		lastKnownNows.put(sensorID, lastKnownNow);
	}

	

}
