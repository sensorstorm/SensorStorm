package nl.tno.sensortimeseries.serializer;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import nl.tno.sensortimeseries.model.Measurement;
import nl.tno.sensortimeseries.model.TimerTick;
import nl.tno.timeseries.model.Streamable;
import nl.tno.timeseries.serializer.StreamableSerializer;

import org.apache.commons.lang.SerializationException;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class MeasurementSerializer extends StreamableSerializer<Measurement> implements Serializable {
	private static final long serialVersionUID = -1141740286906726443L;
	public static final String VALUECLASS_FIELD = "vc";
	public static final String VALUE_FIELD = "v";
	private static final String MEASUREMENT_TIMERTICK_MARKER 	= "Tick";
	private static final String MEASUREMENT_DOUBLE_MARKER 		= "Double";

	
	@Override
	protected void writeObject(Kryo kryo, Output output, Measurement measurement) throws Exception {
		if (measurement.getValueClass().isAssignableFrom(TimerTick.class)) {
			output.writeString(MEASUREMENT_TIMERTICK_MARKER);
			// timertick has no value
		} else if (measurement.getValueClass().isAssignableFrom(Double.class)) {
			output.writeString(MEASUREMENT_DOUBLE_MARKER);
			output.writeDouble((Double)measurement.getValue());
		} else {
			throw new SerializationException("No write serializer avaliable for measurement value class "+measurement.getValueClass().getName());
		}
	}

	@Override
	protected Measurement readObject(Kryo kryo, Input input, Class<Measurement> clas, String streamId, long sequenceNr) throws Exception {
		String marker = input.readString();
		if ((marker == null) || (marker.isEmpty())) {
			throw new SerializationException("No serializer marker in deserialization tuple stream.");
		} else {
			if (MEASUREMENT_TIMERTICK_MARKER.equals(marker)) {
				return new Measurement(streamId, sequenceNr, null, TimerTick.class);
			} else if (MEASUREMENT_DOUBLE_MARKER.equals(marker)) {
				double value = input.readDouble();
				return new Measurement(streamId, sequenceNr, value, Double.class);
			} else {
				throw new SerializationException("Can not deserialize Measurement. No serializer avaliable for value class "+marker);
			}
		}
	}

	
	@Override
	protected Measurement createObject(Tuple tuple) throws IOException {
		Measurement result = null; 
		
		String sensorid = tuple.getStringByField(STREAMID);
		long timestamp = tuple.getLongByField(SEQUENCENR);
		Class<?> valueClass = (Class<?>)tuple.getValueByField(VALUECLASS_FIELD);
		Object valueObject = tuple.getValueByField(VALUE_FIELD);
		
		if (valueClass.isAssignableFrom(TimerTick.class)) {
			result = new Measurement(sensorid, timestamp, null, TimerTick.class);
		} else if (valueClass.isAssignableFrom(Double.class)) {
			result = new Measurement(sensorid, timestamp, (Double)valueObject, Double.class);
		} else {
			throw new SerializationException("Can not create Measurement from tuple. No serializer avaliable for value class "+valueClass);
		}
		return result;
	}

	
	@Override
	protected Values getValues(Streamable object) throws IOException {
		Measurement measurement = (Measurement)object;
		return new Values(measurement.getValueClass(), measurement.getValue());
	}

	@Override
	protected List<String> getTypeFields() {
		List<String> fields = new ArrayList<String>();
		fields.add(VALUECLASS_FIELD);
		fields.add(VALUE_FIELD);
		return fields;
	}
	

}
