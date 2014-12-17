package nl.tno.timeseries.sensor.measurements;

import nl.tno.sensorstorm.mapper.annotation.TupleField;
import nl.tno.sensorstorm.particles.AbstractMetaParticle;
import nl.tno.sensorstorm.particles.DataParticle;

public class Measurement<T> extends AbstractMetaParticle implements DataParticle, Comparable<Measurement<?>> {

	@TupleField
	T value;
	
	public Measurement() {
	}
	
	public Measurement(String channelId, long timestamp, T value) {
		setChannelId(channelId);
		setTimestamp(timestamp);
		this.value = value;
	}

	@Override
	public int compareTo(Measurement<?> o) {
		return Long.signum(timestamp - o.timestamp);
	}

	public T getValue() {
		return value;
	}
	
	@Override
	public String toString() {
		return "M["+channelId+","+timestamp+","+value+"]";
	}
	
	public String toJson() {
		String result = "{\"sensor_id\": \""+channelId+"\", \"utc_epoch_timestamp\": "+timestamp+", \"value\": ";
		if (value instanceof Number) {
			result = result + value;
		} else if (value instanceof String) {
			result = result + "\"" + value + "\"";
		}
		result = result + "}";
		
		return result;
	}
	
}
