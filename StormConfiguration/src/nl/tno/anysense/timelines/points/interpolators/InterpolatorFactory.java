package nl.tno.anysense.timelines.points.interpolators;

public class InterpolatorFactory {
	private static InterpolatorMap linearInterpolators = new InterpolatorMap();
	private static InterpolatorMap sampleHoldInterpolators = new InterpolatorMap();

	static {
		linearInterpolators.put(Long.class, new LinearLongInterpolator());
		linearInterpolators.put(Double.class, new LinearDoubleInterpolator());

		sampleHoldInterpolators.put(Long.class,
				new SampleHoldInterpolator<Long>());
		sampleHoldInterpolators.put(Double.class,
				new SampleHoldInterpolator<Double>());
		sampleHoldInterpolators.put(Boolean.class,
				new SampleHoldInterpolator<Boolean>());
		sampleHoldInterpolators.put(String.class,
				new SampleHoldInterpolator<String>());
	}

	public static <T> InterpolatorInterface<T> getLinearInterpolator(
			Class<T> clazz) {
		return linearInterpolators.get(clazz);
	}

	public static <T> InterpolatorInterface<T> getSampleHoldInterpolator(
			Class<T> clazz) {
		return sampleHoldInterpolators.get(clazz);
	}

	public static <T> InterpolatorInterface<T> getInterpolator(
			String interpolation, Class<T> clazz) throws ClassNotFoundException {
		InterpolatorInterface<T> result;
		if (interpolation.equalsIgnoreCase("samplehold")) {
			result = getSampleHoldInterpolator(clazz);
		} else if (interpolation.equalsIgnoreCase("linear")) {
			result = getLinearInterpolator(clazz);
		} else {
			result = null;
		}

		if (result != null) {
			return result;
		} else {
			throw new ClassNotFoundException();
		}

	}

}
