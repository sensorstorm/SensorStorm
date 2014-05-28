package nl.tno.timeseries.mapper;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.SortedMap;

import nl.tno.timeseries.interfaces.Particle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * The ParticleClassInfo contains information en methods on how to map Particles
 * without a custom mapper
 */
public class ParticleClassInfo {

	private static Logger log = LoggerFactory.getLogger(ParticleClassInfo.class);

	// key = name of field, value = name in tuple
	private SortedMap<String, String> fields;
	private Fields outputFields;
	private Class<?> clazz;;

	public ParticleClassInfo(Class<?> clazz, SortedMap<String, String> fields) {
		this.fields = fields;
		this.clazz = clazz;
		ArrayList<String> copy = new ArrayList<>();
		copy.add(ParticleMapper.STREAM_ID);
		copy.add(ParticleMapper.SEQUENCE_NR);
		copy.add(ParticleMapper.PARTICLE_CLASS);
		copy.addAll(this.fields.values());
		outputFields = new Fields(copy);
	}

	@SuppressWarnings("unchecked")
	public <T> T tupleToParticle(Tuple tuple, Class<T> clazz) {
		try {
			assert clazz.equals(this.clazz);
			Class<? extends Particle> particleClass = (Class<? extends Particle>) Class.forName(tuple.getString(2));
			Particle particle = particleClass.newInstance();
			particle.setChannelId(tuple.getString(0));
			particle.setSequenceNr(tuple.getLong(1));
			for (Entry<String, String> e : fields.entrySet()) {
				Field declaredField = particleClass.getDeclaredField(e.getKey());
				declaredField.setAccessible(true);
				declaredField.set(particle, tuple.getValueByField(e.getValue()));
			}
			return (T) particle;
		} catch (ClassNotFoundException | IllegalAccessException | NoSuchFieldException | SecurityException e) {
			log.error("Was not able to map tuple to particle", e);
			return null;
		} catch (InstantiationException e) {
			throw new IllegalArgumentException("Particles should always have at least an empty constructor");
		}
	}

	public Values particleToValues(Particle particle) {
		try {
			Values v = new Values();
			v.add(particle.getChannelId());
			v.add(particle.getSequenceNr());
			v.add(particle.getClass().getName());
			for (String field : fields.keySet()) {
				Field declaredField = clazz.getDeclaredField(field);
				declaredField.setAccessible(true);
				v.add(declaredField.get(particle));
			}
			return v;
		} catch (IllegalArgumentException | IllegalAccessException | NoSuchFieldException | SecurityException e) {
			log.error("Was not able to map particle to value", e);
			return null;
		}
	}

	public Fields getFields() {
		return outputFields;
	}

}
