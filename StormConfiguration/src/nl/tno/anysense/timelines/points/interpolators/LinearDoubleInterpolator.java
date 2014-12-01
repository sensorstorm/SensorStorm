package nl.tno.anysense.timelines.points.interpolators;

public class LinearDoubleInterpolator implements InterpolatorInterface<Double> {

	@Override
	public Double interpolate(long timestamp1, Double value1, Long timestamp2,
			Double value2, long timestampA) {
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

		// either null -> can not linear interpolate
		if ((value1 == null) || (value2 == null)) {
			return null;
		}

		double part = (value2 - value1) / (timestamp2 - timestamp1);
		double newValue = value1 + (timestampA - timestamp1) * part;

		return newValue;
	}

}
