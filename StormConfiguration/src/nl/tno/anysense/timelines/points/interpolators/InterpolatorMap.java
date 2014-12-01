package nl.tno.anysense.timelines.points.interpolators;

import java.util.HashMap;
import java.util.Map;

public class InterpolatorMap {

	private final Map<Class<?>, InterpolatorInterface<?>> map;

	public InterpolatorMap() {
		map = new HashMap<Class<?>, InterpolatorInterface<?>>();
	}

	@SuppressWarnings("unchecked")
	public <T> InterpolatorInterface<T> get(Class<T> clazz) {
		return (InterpolatorInterface<T>) map.get(clazz);
	}

	public <T> void put(Class<T> clazz, InterpolatorInterface<T> value) {
		try {
			map.put(clazz, value);
		} catch (SecurityException e) {
			throw new RuntimeException(e);
		}
	}
}
