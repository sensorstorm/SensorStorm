package nl.tno.anysense.timelines.holder;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

import nl.tno.anysense.timelines.points.interpolators.InterpolatorFactory;
import nl.tno.anysense.timelines.points.interpolators.InterpolatorInterface;

/**
 * Holder for the measurements with value type V
 * 
 * @author waaijbdvd
 * 
 * @param <V>
 */
public class MeasurementsHolder<V> {
	public enum ValueTypes {
		UnknownType, DoubleType, LongType, StringType, BlobType
	};

	protected ConcurrentSkipListMap<Long, Object> measurements;
	protected Class<V> clazz;
	protected String timelineName;

	public static <V> MeasurementsHolder<V> create(Class<V> clazz,
			String timelineName) {
		return new MeasurementsHolder<V>(clazz, timelineName);
	}

	/**
	 * Create a new holder for a measurements with value type V
	 */
	public MeasurementsHolder(Class<V> clazz, String timelineName)
			throws MeasurementException {
		if (clazz == null) {
			throw new MeasurementException(
					"Null class is not supported as valuetype.");
		}

		// is clazz supported?
		if (!isClazzSupported(clazz)) {
			throw new MeasurementException("Class " + clazz.getName()
					+ " not supported as valuetype.");
		}

		if ((timelineName == null) || (timelineName.isEmpty())) {
			throw new MeasurementException("Timelinename may no be empty.");
		}

		this.clazz = clazz;
		this.timelineName = timelineName;

		measurements = new ConcurrentSkipListMap<Long, Object>();
	}

	/**
	 * Verifies if the clazz is of a suported classtype: long, double, string,
	 * blob
	 * 
	 * @param clazz
	 * @return true or false
	 */
	private boolean isClazzSupported(Class<V> clazz) {
		if (clazz.isAssignableFrom(Long.class)
				|| clazz.isAssignableFrom(String.class)
				|| clazz.isAssignableFrom(Double.class)
				|| clazz.isAssignableFrom(Blob.class)) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Return the name of the valuetype of this TimelineHolder elements. The
	 * following options are available: - long - string - double - blob -
	 * unknown
	 * 
	 * @return
	 */
	public String getValueTypeName() {
		if (clazz.isAssignableFrom(Long.class)) {
			return "long";
		} else if (clazz.isAssignableFrom(Double.class)) {
			return "double";
		} else if (clazz.isAssignableFrom(String.class)) {
			return "string";
		} else if (clazz.isAssignableFrom(Blob.class)) {
			return "blob";
		} else {
			return "unknown (" + clazz.getName() + ")";
		}
	}

	public static ValueTypes makeValueType(String valueTypeName) {
		if ((valueTypeName == null) || (valueTypeName.isEmpty()))
			return ValueTypes.UnknownType;

		if (valueTypeName.equalsIgnoreCase("double"))
			return ValueTypes.DoubleType;
		else if (valueTypeName.equalsIgnoreCase("long"))
			return ValueTypes.LongType;
		else if (valueTypeName.equalsIgnoreCase("text"))
			return ValueTypes.StringType;
		else if (valueTypeName.equalsIgnoreCase("string")) // just to be safe
			return ValueTypes.StringType;
		else if (valueTypeName.equalsIgnoreCase("blob"))
			return ValueTypes.BlobType;
		else
			return ValueTypes.UnknownType;
	}

	public Class<V> getValueTypeClass() {
		return clazz;
	}

	public String getTimelineName() {
		return timelineName;
	}

	public void setTimelineName(String timelineName) {
		this.timelineName = timelineName;
	}

	/**
	 * Returns the value of the measurement on the given timestamp Returns null
	 * if the timestamp is before the first timestamp available in this holder.
	 * 
	 * @param timestamp
	 * @return
	 */
	public V getValue(long timestamp) {
		return getValueFromEntry(measurements.floorEntry(timestamp));
	}

	/**
	 * Get all measurements
	 * 
	 * @return
	 */
	public Set<Map.Entry<Long, V>> getMeasurements() {
		Set<Map.Entry<Long, Object>> entrySet = measurements.entrySet();

		HashMap<Long, V> map = new HashMap<Long, V>();
		Iterator<Entry<Long, Object>> measurementsIterator = entrySet
				.iterator();
		while (measurementsIterator.hasNext()) {
			Entry<Long, Object> measurementEntry = measurementsIterator.next();
			map.put(measurementEntry.getKey(),
					getValueFromEntry(measurementEntry));
		}
		return map.entrySet();
	}

	public void putValue(long timestamp, V value) {
		if (clazz.isAssignableFrom(Double.class)) {
			DoubleValueObject doubleValue = new DoubleValueObject(
					(Double) value);
			measurements.put(timestamp, doubleValue);
		} else if (clazz.isAssignableFrom(Long.class)) {
			LongValueObject longValue = new LongValueObject((Long) value);
			measurements.put(timestamp, longValue);
		} else if (clazz.isAssignableFrom(String.class)) {
			StringValueObject stringValue = new StringValueObject(
					(String) value);
			measurements.put(timestamp, stringValue);
		} else if (clazz.isAssignableFrom(Blob.class)) {
			BlobValueObject blobValue = new BlobValueObject((Blob) value);
			measurements.put(timestamp, blobValue);
		} else
			throw new MeasurementException(
					"InternalError: Can not store value of type "
							+ clazz.getName());
	}

	public void putValue(int timestamp, V value) {
		putValue((long) timestamp, value);
	}

	public int size() {
		return measurements.size();
	}

	/**
	 * Return the first timestamp, or null if no measurements are available.
	 * 
	 * @return
	 */
	public Long getFirstTimestamp() {
		try {
			return measurements.firstKey();
		} catch (NoSuchElementException e) {
			return null;
		}
	}

	/**
	 * Return the last timestamp, or null if no measurements are available.
	 * 
	 * @return
	 */
	public Long getLastTimestamp() {
		try {
			return measurements.lastKey();
		} catch (NoSuchElementException e) {
			return null;
		}
	}

	/**
	 * Returns the timestamp of the first measurement before the given timestamp
	 * 
	 * @param timestamp
	 * @return First timestamp before, or null
	 */
	public Map.Entry<Long, V> getMeasurementBefore(long timestamp) {
		Entry<Long, Object> floorMeasurement = measurements
				.floorEntry(timestamp);

		if (floorMeasurement == null) {
			return new AbstractMap.SimpleEntry<Long, V>(timestamp, null);
		} else {
			Object value = floorMeasurement.getValue();
			Long valueTimestamp = floorMeasurement.getKey();
			V valueFromValueObject = getValueFromValueObject(value);
			return new AbstractMap.SimpleEntry<Long, V>(valueTimestamp,
					valueFromValueObject);
		}
	}

	/**
	 * Returns the timestamp of the first measurement after the given timestamp
	 * 
	 * @param timestamp
	 * @return First timestamp after, or null
	 */
	public Map.Entry<Long, V> getMeasurementAfter(long timestamp) {
		Entry<Long, Object> ceilingMeasurement = measurements
				.ceilingEntry(timestamp);

		if (ceilingMeasurement == null) {
			return new AbstractMap.SimpleEntry<Long, V>(timestamp, null);
		} else {
			Object value = ceilingMeasurement.getValue();
			Long valueTimestamp = ceilingMeasurement.getKey();
			V valueFromValueObject = getValueFromValueObject(value);
			return new AbstractMap.SimpleEntry<Long, V>(valueTimestamp,
					valueFromValueObject);
		}
	}

	/**
	 * Merge the mergingMeasurements with the current measurements up to the
	 * forkTimestamp
	 * 
	 * @param mergingMeasurements
	 * @param forkTimestamp
	 * @throws MeasurementException
	 */
	public void mergeMeasurements(MeasurementsHolder<?> mergingMeasurements,
			long forkTimestamp) throws MeasurementException {
		if (mergingMeasurements.clazz != clazz) {
			throw new MeasurementException(
					"Can not merge measurements of type "
							+ mergingMeasurements.clazz.getName()
							+ " with type " + clazz.getName());
		}

		// now we are sure that measurements and mergeMeasurements are of the
		// same type (clazz)
		Long timestamp;
		ConcurrentSkipListMap<Long, Object> from = measurements;
		ConcurrentSkipListMap<Long, Object> merging = mergingMeasurements.measurements;
		Set<Entry<Long, Object>> mergingEntrySet = merging.entrySet();
		for (Entry<Long, Object> mergingEntry : mergingEntrySet) {
			timestamp = mergingEntry.getKey();
			if (timestamp < forkTimestamp) { // overwrite
				from.put(timestamp, mergingEntry.getValue());
			} else { // keep value of higher leave in the timelineName tree
				break;
			}
		}
	}

	/**
	 * Generate points based on this measurementHolder
	 * 
	 * @param measurementsIncludingFromTo
	 * @param fromTimestamp
	 * @param toTimestamp
	 * @param maxCount
	 * @param nrOfPoints
	 * @param interpolation
	 * @return
	 * @throws MeasurementException
	 */
	public PointsHolder<V> calculatePointsFromMeasurements(Long fromTimestamp,
			Long toTimestamp, int nrOfPoints, String interpolation)
			throws MeasurementException {

		// create timelineHolder for the points to be returned.
		PointsHolder<V> result = new PointsHolder<V>(this);

		Long firstTimestamp = getFirstTimestamp();
		if (firstTimestamp == null)
			firstTimestamp = 0L;
		Long lastTimestamp = getLastTimestamp();
		if (lastTimestamp == null)
			lastTimestamp = Long.MAX_VALUE;

		V value;
		long timestamp;
		double windowSize = toTimestamp - fromTimestamp;
		double resolutionStep;
		int startIndex;
		if (nrOfPoints == 1) { // only one point -> take the middle one
			// avoid the first and last point, only take the middle one and
			// interpolate around the nearest neighbours
			resolutionStep = windowSize / 2.0;
			startIndex = 1; // avoid the first one
			nrOfPoints = 2; // avoid the for loop to close
		} else {
			resolutionStep = windowSize / (nrOfPoints - 1.0);
			startIndex = 0;
		}
		if (resolutionStep == 0) {
			throw new MeasurementException("Number of Points (" + nrOfPoints
					+ ") is too large for the time window size(" + windowSize
					+ ") resulting in stepsize 0");
		}

		// get interpolators
		InterpolatorInterface<V> interpolator;
		try {
			interpolator = InterpolatorFactory.getInterpolator(interpolation,
					getValueTypeClass());
		} catch (ClassNotFoundException e) {
			throw new MeasurementException("the requested interpolation ("
					+ interpolation
					+ ") is not available for the measurementtype ("
					+ getValueTypeName() + ") of the observer.");
		}

		// interpolate the number of requested points.
		int endIndex = nrOfPoints;
		for (int i = startIndex; i < endIndex; i++) {
			timestamp = fromTimestamp + (long) (i * resolutionStep);

			if ((timestamp >= firstTimestamp)) {
				Entry<Long, V> measurementBefore = getMeasurementBefore(timestamp);
				Entry<Long, V> measurementAfter = getMeasurementAfter(timestamp);

				if (measurementBefore == null) {
					value = null;
				} else if (measurementAfter == null) {
					value = interpolator
							.interpolate(measurementBefore.getKey(),
									measurementBefore.getValue(), null, null,
									timestamp);
				} else {
					value = interpolator.interpolate(
							measurementBefore.getKey(),
							measurementBefore.getValue(),
							measurementAfter.getKey(),
							measurementAfter.getValue(), timestamp);
				}
			} else {
				value = null;
			}

			result.putValue(timestamp, value);
		}

		return result;
	}

	@Override
	public String toString() {
		StringBuilder result = new StringBuilder();
		result.append("Measurements[" + measurements.size() + ","
				+ timelineName + "] ");
		Map.Entry<Long, Object> measurement = null;
		Iterator<Entry<Long, Object>> measurementsIterator = measurements
				.entrySet().iterator();
		while (measurementsIterator.hasNext()) {
			measurement = measurementsIterator.next();
			result.append("[T:");
			result.append(measurement.getKey());
			result.append(",V:");
			result.append(formatValue(measurement.getValue()));
			result.append("]");

			if (measurementsIterator.hasNext())
				result.append(",");
		}
		return result.toString();
	}

	/**
	 * Make a formatted String of the valueObject
	 * 
	 * @param valueObject
	 * @return
	 */
	protected StringBuilder formatValue(Object valueObject) {
		V value = getValueFromValueObject(valueObject);

		StringBuilder result = new StringBuilder();
		if (value == null) {
			result.append("null");
		} else if (value instanceof Number) {
			result.append(value);
		} else if (value instanceof String) {
			result.append('\"');
			result.append(value);
			result.append('\"');
		} else if (value instanceof Blob) {
			result.append("blob[" + ((Blob) value).blobID + "]");
		} else
			result.append(value);

		return result;
	}

	/**
	 * Returns the value, in the correct Java class, out of the given entry
	 * 
	 * @param measurementEntry
	 * @return
	 */
	protected V getValueFromEntry(Entry<Long, Object> measurementEntry) {
		if (measurementEntry == null)
			return null;
		else {
			Object value = measurementEntry.getValue();
			return getValueFromValueObject(value);
		}
	}

	@SuppressWarnings("unchecked")
	protected V getValueFromValueObject(Object value) {
		if (value instanceof DoubleValueObject) {
			return (V) ((DoubleValueObject) value).value;
		} else if (value instanceof LongValueObject) {
			return (V) ((LongValueObject) value).value;
		} else if (value instanceof StringValueObject) {
			return (V) ((StringValueObject) value).value;
		} else if (value instanceof BlobValueObject) {
			return (V) ((BlobValueObject) value).value;
		} else
			throw new MeasurementException(
					"Internal error: Unknown measurementtype in MeasurementHolder: "
							+ value.getClass().getName());
	}

}
