package nl.tno.anysense.timelines.holder;


/**
 * Holder for the measurements/points of a timeline with value type V
 * 
 * @author waaijbdvd
 * 
 * @param <V>
 */
public class PointsHolder<V> extends MeasurementsHolder<V> {

	public static <V> PointsHolder<V> create(Class<V> clazz, String timelineName) {
		return new PointsHolder<V>(clazz, timelineName);
	}

	/**
	 * Create a new holder for a timeline with value type V
	 */
	public PointsHolder(Class<V> clazz, String timelineName)
			throws MeasurementException {
		super(clazz, timelineName);
	}

	public PointsHolder(MeasurementsHolder<V> measurementsHolder) {
		super(measurementsHolder.getValueTypeClass(), measurementsHolder
				.getTimelineName());
	}

}
