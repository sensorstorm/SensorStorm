package nl.tno.timeseries.mapper;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import nl.tno.timeseries.mapper.annotation.Mapper;
import nl.tno.timeseries.mapper.annotation.TupleField;
import nl.tno.timeseries.mapper.api.CustomParticlePojoMapper;
import nl.tno.timeseries.mapper.api.Particle;
import nl.tno.timeseries.test.AutoMappedParticle;
import nl.tno.timeseries.test.DoubleMeasurement;
import nl.tno.timeseries.test.SelfMappedParticle;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class ParticleMapper {

	public static final String SEQUENCE_NR = "sequenceNr";
	public static final String STREAM_ID = "streamId";
	public static final String PARTICLE_CLASS = "particleClass";

	private static ConcurrentMap<Class<?>, CustomParticlePojoMapper<?>> customSerializers = new ConcurrentHashMap<>();
	private static ConcurrentMap<Class<?>, ParticleClassInfo> particleClassInfos = new ConcurrentHashMap<>();

	public static Values serialize(Particle particle) {
		Class<? extends Particle> clazz = particle.getClass();
		if (hasCustomSerializer(clazz)) {
			// TODO is there an easier way to do this?
			// TODO cache method, figuring out the serialize method is a constant overhead
			Method serializeMethod = null;
			CustomParticlePojoMapper<?> customSerializer = getCustomSerializer(clazz);
			for (Method m : customSerializer.getClass().getMethods()) {
				if (m.getName().equals("serialize")) {
					serializeMethod = m;
				}
			}
			try {
				return (Values) serializeMethod.invoke(customSerializer,
						particle);
			} catch (IllegalAccessException | IllegalArgumentException
					| InvocationTargetException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			}
			// return customSerializer.serialize(particle);
		} else {
			return getParticleClassInfo(clazz).serialize(particle);
		}
	}

	@SuppressWarnings("unchecked")
	public static <T extends Particle> T deserialize(Tuple tuple, Class<T> clazz) {
		if (hasCustomSerializer(clazz)) {
			return (T) getCustomSerializer(clazz).deserialize(tuple);
		} else {
			return getParticleClassInfo(clazz).deserialize(tuple, clazz);
		}
	}

	// TODO is it faster to look up the class in the map?
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
			return getCustomSerializer(clazz).getFields();
		} else {
			return getParticleClassInfo(clazz).getFields();
		}
	}

	private static ParticleClassInfo getParticleClassInfo(Class<?> clazz) {
		ParticleClassInfo pci = particleClassInfos.get(clazz);
		if (pci != null) {
			return pci;
		} else {
			// Construct the ParticleClassInfo object.
			// We also do the static validation here
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

	private static CustomParticlePojoMapper<?> getCustomSerializer(
			Class<?> clazz) {
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

	public static void main(String[] args) {
		AutoMappedParticle p = new AutoMappedParticle();
		p.id = "id";
		p.intId = 14;
		p.sequenceNr = 100;
		p.streamId = "Stream-1";
		p.map = new HashMap<>();
		System.out.println(ParticleMapper.getFields(p.getClass()));
		Values serialized = ParticleMapper.serialize(p);
		System.out.println(serialized);

		SelfMappedParticle s = new SelfMappedParticle();
		s.id = "id";
		s.intId = 14;
		s.sequenceNr = 100;
		s.streamId = "Stream-1";
		s.map = new HashMap<>();
		System.out.println(ParticleMapper.getFields(s.getClass()));
		System.out.println(ParticleMapper.serialize(s));
		
		DoubleMeasurement dm = new DoubleMeasurement();
		dm.setSequenceNr(123);
		dm.setStreamId("StreamID");
		dm.setValue(15);
		System.out.println(ParticleMapper.getFields(dm.getClass()));
		System.out.println(ParticleMapper.serialize(dm));
		try {
			DoubleMeasurement.class.newInstance();
		} catch (InstantiationException | IllegalAccessException e) {
			throw new IllegalArgumentException("Particles should always have at least an empty constructor");
		}
	}

}
