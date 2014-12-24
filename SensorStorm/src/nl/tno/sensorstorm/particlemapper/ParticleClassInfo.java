package nl.tno.sensorstorm.particlemapper;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;

import nl.tno.sensorstorm.api.particles.Particle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * The ParticleClassInfo contains information and methods on how to map
 * {@link Particle}s without a custom mapper.
 */
public class ParticleClassInfo {

	private static Logger log = LoggerFactory
			.getLogger(ParticleClassInfo.class);

	// key = name of field, value = name in tuple
	private final SortedMap<String, String> fields;
	private final Fields outputFields;
	private final Class<?> clazz;;

	/**
	 * Construct a {@link ParticleClassInfo} for a type of {@link Particle}.
	 * 
	 * @param clazz
	 *            {@link Class} of this {@link Particle}
	 * @param fields
	 *            Map with as key the name of the field in the {@link Particle},
	 *            value the name in the {@link Tuple}
	 */
	public ParticleClassInfo(Class<?> clazz, SortedMap<String, String> fields) {
		this.fields = fields;
		this.clazz = clazz;
		List<String> copy = new ArrayList<>();
		copy.add(ParticleMapper.TIMESTAMP_FIELD_NAME);
		copy.add(ParticleMapper.PARTICLE_CLASS_FIELD_NAME);
		copy.addAll(this.fields.values());
		outputFields = new Fields(copy);
	}

	/**
	 * Map a {@link Tuple} into a {@link Particle}.
	 * 
	 * @param <T>
	 *            Type of the {@link Particle}
	 * @param tuple
	 *            {@link Tuple} to be mapped
	 * @param clazz
	 *            {@link Class} of the {@link Particle}, should be equal to the
	 *            {@link Class} passed in the constructor
	 * @return The mapped {@link Particle}
	 */
	@SuppressWarnings("unchecked")
	public <T> T tupleToParticle(Tuple tuple, Class<T> clazz) {
		try {
			assert clazz.equals(this.clazz);
			Class<? extends Particle> particleClass = (Class<? extends Particle>) Class
					.forName(tuple.getString(ParticleMapper.PARTICLE_CLASS_IDX));
			Particle particle = particleClass.newInstance();
			particle.setTimestamp(tuple.getLong(ParticleMapper.TIMESTAMP_IDX));
			for (Entry<String, String> e : fields.entrySet()) {
				Field declaredField = particleClass
						.getDeclaredField(e.getKey());
				declaredField.setAccessible(true);
				declaredField
						.set(particle, tuple.getValueByField(e.getValue()));
			}
			return (T) particle;
		} catch (ClassNotFoundException | IllegalAccessException
				| NoSuchFieldException | SecurityException e) {
			log.error(
					"Was unable to map tuple to particle: " + tuple.toString(),
					e);
			return null;
		} catch (InstantiationException e) {
			throw new IllegalArgumentException(
					"Particles should always have an empty constructor");
		}
	}

	/**
	 * Map a {@link Particle} to a {@link Values} object.
	 * 
	 * @param particle
	 *            {@link Particle} to map
	 * @return Mapped {@link Particle}
	 */
	public Values particleToValues(Particle particle) {
		try {
			Values v = new Values();
			// see constant TIMESTAP_IDX
			v.add(particle.getTimestamp());
			// see constant PARTICLE_CLASS_IDX
			v.add(particle.getClass().getName());
			for (String field : fields.keySet()) {
				Field declaredField = clazz.getDeclaredField(field);
				declaredField.setAccessible(true);
				v.add(declaredField.get(particle));
			}
			return v;
		} catch (IllegalArgumentException | IllegalAccessException
				| NoSuchFieldException | SecurityException e) {
			log.error("Was not able to map particle to value", e);
			return null;
		}
	}

	/**
	 * {@link Fields} object describing the {@link Particle}.
	 * 
	 * @return {@link Fields} with fields the {@link Tuple}
	 */
	public Fields getFields() {
		return outputFields;
	}

}
