package nl.tno.anysense.timelines.points.interpolators;

public interface InterpolatorInterface<T> {

	/**
	 * Calculate Ya where Xa lies between two points (x1,y1) and (x2,y2)
	 * 
	 * Note: Timestamp2 is a Long object and can be null, indicating there is no
	 * end timestamp Timestamp1 can never be null, there is always a begin in
	 * time.
	 * 
	 * @param timestamp1
	 *            point x1
	 * @param value1
	 *            point y1
	 * @param timestamp2
	 *            point x2
	 * @param value2
	 *            point y2
	 * @param timstampA
	 *            Timestamp of calculated value
	 * @return
	 */
	public T interpolate(long timestamp1, T value1, Long timestamp2, T value2,
			long timestampA);

}
