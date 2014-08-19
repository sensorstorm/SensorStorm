package nl.tno.timeseries.mapper;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentMap;

import nl.tno.timeseries.interfaces.Particle;
import nl.tno.timeseries.mapper.annotation.Mapper;
import nl.tno.timeseries.mapper.annotation.TupleField;
import nl.tno.timeseries.mapper.api.CustomParticlePojoMapper;

import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class ParticleMapper {

	public static final Logger log = LoggerFactory
			.getLogger(ParticleMapper.class);

	public static final String SEQUENCE_NR = "sequenceNr";
	public static final String STREAM_ID = "streamId";
	public static final String PARTICLE_CLASS = "particleClass";

	private static ConcurrentMap<Class<?>, CustomParticlePojoMapper<?>> customSerializers = new ConcurrentHashMap<>();
	private static ConcurrentMap<Class<?>, Method> customSerializersMapMethods = new ConcurrentHashMap<>();
	private static ConcurrentMap<Class<?>, ParticleClassInfo> particleClassInfos = new ConcurrentHashMap<>();

	public static Values particleToValues(Particle particle) {
		Class<? extends Particle> clazz = particle.getClass();
		if (hasCustomSerializer(clazz)) {
			CustomParticlePojoMapper<?> customSerializer = getCustomMapper(clazz);
			Method serializeMethod = customSerializersMapMethods.get(clazz);
			try {
				return (Values) serializeMethod.invoke(customSerializer,
						particle);
			} catch (IllegalAccessException | IllegalArgumentException
					| InvocationTargetException e) {
				log.error("Could not map particle to values", e);
				return null;
			}
		} else {
			return getParticleClassInfo(clazz).particleToValues(particle);
		}
	}

	public static Values particleToValues(Particle particle,
			int expectedNrOfFields) {
		Values values = particleToValues(particle);
		if (values.size() > expectedNrOfFields) {
			throw new IllegalArgumentException("Expected number of Fields ("
					+ expectedNrOfFields
					+ ") is smaller than the found number of fields ("
					+ values.size() + ")");
		}
		while (values.size() < expectedNrOfFields) {
			values.add(null);
		}
		return values;
	}

	@SuppressWarnings("unchecked")
	public static <T extends Particle> T tupleToParticle(Tuple tuple,
			Class<T> clazz) {
		if (hasCustomSerializer(clazz)) {
			return (T) getCustomMapper(clazz).tupleToParticle(tuple);
		} else {
			return getParticleClassInfo(clazz).tupleToParticle(tuple, clazz);
		}
	}

	public static Particle tupleToParticle(Tuple tuple) {
		Class<?> clazz = null;
		ParticleClassInfo particleClassInfo;
		try {
			clazz = Class.forName(tuple.getString(2));
			particleClassInfo = getParticleClassInfo(clazz);
		} catch (ClassNotFoundException e) {
			particleClassInfo = null;
		}
		if (particleClassInfo != null) {
			return (Particle) particleClassInfo.tupleToParticle(tuple, clazz);
		} else {
			// Maybe it has a custom mapper?
			for (CustomParticlePojoMapper<?> m : customSerializers.values()) {
				if (m.canMapTuple(tuple)) {
					return m.tupleToParticle(tuple);
				}
			}
			// Could not find a custom mapper. Now what?
			log.error("Could not find mapper for the tuple. If the particle has a custom mapper that the ParticleMapper doesn't know, tell the ParticleMapper by calling the ParticleMapper.inspectClass method.");
			return null;
		}
	}

	private static boolean hasCustomSerializer(Class<? extends Particle> clazz) {
		for (Annotation a : clazz.getAnnotations()) {
			if (a instanceof Mapper) {
				return true;
			}
		}
		return false;
	}

	public static Fields getFields(Class<? extends Particle> clazz) {
		if (hasCustomSerializer(clazz)) {
			return getCustomMapper(clazz).getFields();
		} else {
			return getParticleClassInfo(clazz).getFields();
		}
	}

	public void inspectClass(Class<? extends Particle> clazz) {
		getFields(clazz);
	}

	private static ParticleClassInfo getParticleClassInfo(Class<?> clazz) {
		ParticleClassInfo pci = particleClassInfos.get(clazz);
		if (pci != null) {
			return pci;
		} else {
			// Construct the ParticleClassInfo object.
			// key = name of field, value = name in tuple
			SortedMap<String, String> outputFields = new TreeMap<String, String>();
			for (Field f : clazz.getDeclaredFields()) {
				f.setAccessible(true);
				for (Annotation a : f.getAnnotations()) {
					if (a instanceof TupleField) {
						String name = ((TupleField) a).name();
						if (name == null || name.length() == 0) {
							name = f.getName();
						}
						outputFields.put(f.getName(), name);
					}
				}
			}

			pci = new ParticleClassInfo(clazz, outputFields);
			particleClassInfos.putIfAbsent(clazz, pci);
			return particleClassInfos.get(clazz);
		}
	}

	private static CustomParticlePojoMapper<?> getCustomMapper(Class<?> clazz) {
		CustomParticlePojoMapper<?> ps = customSerializers.get(clazz);
		if (ps != null) {
			return ps;
		} else {
			try {
				for (Annotation a : clazz.getAnnotations()) {
					if (a instanceof Mapper) {
						Class<?> serializerClass = ((Mapper) a).value();
						customSerializers.putIfAbsent(clazz,
								(CustomParticlePojoMapper<?>) serializerClass
										.newInstance());
						// Find the (first) map method
						CustomParticlePojoMapper<?> customSerializer = getCustomMapper(clazz);
						for (Method m : customSerializer.getClass()
								.getMethods()) {
							if (m.getName().equals("particleToValues")) {
								customSerializersMapMethods.putIfAbsent(clazz,
										m);
								break;
							}
						}
						return customSerializers.get(clazz);
					}
				}
			} catch (InstantiationException | IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		// should not be possible to get here
		return null;
	}

	public static Fields mergeFields(Fields one, Fields two) {
		List<String> copy = one.toList();
		for (String s : two.toList()) {
			if (!copy.contains(s)) {
				copy.add(s);
			}
		}
		return new Fields(copy);
	}

}
