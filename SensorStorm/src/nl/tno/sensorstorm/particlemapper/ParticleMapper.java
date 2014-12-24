package nl.tno.sensorstorm.particlemapper;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentMap;

import nl.tno.sensorstorm.api.annotation.Mapper;
import nl.tno.sensorstorm.api.annotation.TupleField;
import nl.tno.sensorstorm.api.particles.CustomParticlePojoMapper;
import nl.tno.sensorstorm.api.particles.Particle;
import nl.tno.sensorstorm.storm.SensorStormBolt;
import nl.tno.sensorstorm.storm.SensorStormSpout;

import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Utility Class that can map {@link Tuple}s to {@link Particle}s and
 * {@link Particle}s to {@link Values}. The class also has useful related
 * methods.
 */
public class ParticleMapper {

	public static final Logger log = LoggerFactory
			.getLogger(ParticleMapper.class);

	public static final int TIMESTAMP_IDX = 0;
	public static final int PARTICLE_CLASS_IDX = 1;
	public static final int PARTICLE_MINIMAL_FIELDS = 2;

	public static final String TIMESTAMP_FIELD_NAME = "timestamp";
	public static final String PARTICLE_CLASS_FIELD_NAME = "particleClass";

	private static final String PARTICLE_TO_VALUES_METHOD_NAME = "particleToValues";

	private static ConcurrentMap<Class<?>, CustomParticlePojoMapper<?>> customSerializers = new ConcurrentHashMap<>();
	private static ConcurrentMap<Class<?>, Method> customSerializersMapMethods = new ConcurrentHashMap<>();
	private static ConcurrentMap<Class<?>, ParticleClassInfo> particleClassInfos = new ConcurrentHashMap<>();

	/**
	 * Private constructor, this class should not be instantiated.
	 */
	private ParticleMapper() {

	}

	/**
	 * Map a {@link Particle} to a {@link Values} object.
	 * 
	 * @param particle
	 *            {@link Particle} to map
	 * @return Corresponding {@link Values} object
	 */
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

	/**
	 * Map a {@link Particle} to a {@link Values} object and make sure it has
	 * expectedNrOfFields fields. This is useful for when a Spout or Bolt has
	 * declared more output fields than this {@link Particle} has.
	 * 
	 * @param particle
	 *            {@link Particle} to map
	 * @param expectedNrOfFields
	 *            Desired number of fields
	 * @return Corresponding {@link Values} object
	 */
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

	/**
	 * Map a {@link Tuple} to a {@link Particle} when the type of the
	 * {@link Particle} is already known.
	 * 
	 * @param <T>
	 *            The type of the {@link Particle}
	 * @param tuple
	 *            {@link Tuple} to be mapped
	 * @param clazz
	 *            {@link Class} of the expected type of {@link Particle}
	 * @return The mapped {@link Tuple}
	 */
	@SuppressWarnings("unchecked")
	public static <T extends Particle> T tupleToParticle(Tuple tuple,
			Class<T> clazz) {
		if (hasCustomSerializer(clazz)) {
			return (T) getCustomMapper(clazz).tupleToParticle(tuple);
		} else {
			return getParticleClassInfo(clazz).tupleToParticle(tuple, clazz);
		}
	}

	/**
	 * Map a {@link Tuple} to a {@link Particle} when the type of the
	 * {@link Particle} is not yet known.
	 * 
	 * @param tuple
	 *            {@link Tuple} to be mapped
	 * @return The mapped {@link Tuple}
	 */
	public static Particle tupleToParticle(Tuple tuple) {
		Class<?> clazz = null;
		ParticleClassInfo particleClassInfo;
		try {
			clazz = Class.forName(tuple.getString(PARTICLE_CLASS_IDX));
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

	/**
	 * Check if a type of {@link Particle} has a
	 * {@link CustomParticlePojoMapper}.
	 * 
	 * @param clazz
	 *            Type of the {@link Particle}
	 * @return if the {@link Particle} has a custom serializer
	 */
	private static boolean hasCustomSerializer(Class<? extends Particle> clazz) {
		for (Annotation a : clazz.getAnnotations()) {
			if (a instanceof Mapper) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Get the {@link Fields} present in type of {@link Particle}.
	 * 
	 * @param clazz
	 *            {@link Class} of the {@link Particle}
	 * @return {@link Fields} in the {@link Particle}
	 */
	public static Fields getFields(Class<? extends Particle> clazz) {
		if (hasCustomSerializer(clazz)) {
			return getCustomMapper(clazz).getFields();
		} else {
			return getParticleClassInfo(clazz).getFields();
		}
	}

	/**
	 * Inspect a type of {@link Particle}. A {@link Particle} cannot be mapped
	 * until it is known by the {@link ParticleMapper}. No worries, the
	 * {@link SensorStormSpout} and {@link SensorStormBolt} usually take care of
	 * this.
	 * 
	 * @param clazz
	 *            Type of the {@link Particle} to inspect
	 */
	public static void inspectClass(Class<? extends Particle> clazz) {
		getFields(clazz);
	}

	/**
	 * Get the {@link ParticleClassInfo} of a auto-mapped {@link Particle} type.
	 * One is created if it is not present yet.
	 * 
	 * @param clazz
	 *            {@link Class} of the {@link Particle}.
	 * @return {@link ParticleClassInfo} for this type of {@link Particle}
	 */
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
						if ((name == null) || (name.length() == 0)) {
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

	/**
	 * Get the {@link CustomParticlePojoMapper} of a manual-mapped
	 * {@link Particle} type. One is created if it is not present yet.
	 * 
	 * @param clazz
	 *            {@link Class} of the {@link Particle}.
	 * @return {@link CustomParticlePojoMapper} for this type of
	 *         {@link Particle}
	 */
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
							if (m.getName().equals(
									PARTICLE_TO_VALUES_METHOD_NAME)) {
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

	/**
	 * Merge two Fields objects. Duplicate fields are removed in the process. If
	 * one of the arguments is null the method will return a copy of the other
	 * Fields. If both are null the result is an empty Fields object.
	 * 
	 * @param first
	 *            The first Fields object (may be null)
	 * @param second
	 *            The second Fields object (may be null)
	 * @return Fields object
	 */
	public static Fields mergeFields(Fields first, Fields second) {
		List<String> copy;
		if (first == null) {
			copy = new ArrayList<String>();
		} else {
			copy = first.toList();
		}
		if (second != null) {
			for (String s : second.toList()) {
				if (!copy.contains(s)) {
					copy.add(s);
				}
			}
		}
		return new Fields(copy);
	}

	/**
	 * Get the index of a Field in the {@link Tuple} of a {@link Particle}.
	 * 
	 * @param clazz
	 *            Type of {@link Particle}.
	 * @param fieldId
	 *            Name of the field
	 * @return position of the field. -1 if not found.
	 */
	public static int getFieldIdx(Class<? extends Particle> clazz,
			String fieldId) {
		Fields fields = ParticleMapper.getFields(clazz);
		int fieldNr = -1;
		for (String field : fields) {
			fieldNr++;
			if (field != null) {
				if (fieldId.equals(field)) {
					return fieldNr;
				}
			}
		}
		return -1;
	}

}
