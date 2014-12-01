package nl.tno.anysense.timelines.points.interpolators;

public class SampleHoldInterpolator<T> implements InterpolatorInterface<T> {

	@Override
	public T interpolate(long timestamp1, T value1, Long timestamp2, T value2,
			long timestampA) {
		return value1;
	}

}
