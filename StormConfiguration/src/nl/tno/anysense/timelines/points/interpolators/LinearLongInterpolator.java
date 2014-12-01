package nl.tno.anysense.timelines.points.interpolators;

public class LinearLongInterpolator implements InterpolatorInterface<Long> {

	@Override
	public Long interpolate(long timestamp1, Long value1, Long timestamp2,
			Long value2, long timestampA) {
		if (timestamp2 == null) // can not interpolate with no end timestamp
			return null;

		// range invalid
		if (timestamp1 > timestamp2)
			return null;
		// range contains only one value
		if (timestamp1 == timestamp2) {
			return value1;
		}
		// timestampA out of scope?
		if ((timestampA < timestamp1) || (timestampA > timestamp2))
			return null;

		// left null -> samplehold null
		if ((value1 == null) && (value2 != null)) {
			return null;
		}
		// right null -> samplehold value1 (left)
		if ((value1 != null) && (value2 == null)) {
			return value1;
		}
		// both null -> should return null
		if ((value1 == null) && (value2 == null)) {
			return null;
		}

		long part = (value2 - value1) / (timestamp2 - timestamp1);
		long newValue = value1 + ((timestampA - timestamp1) * part);

		return newValue;
	}

}
