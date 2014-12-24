package nl.tno.sensorstorm.storm;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import nl.tno.sensorstorm.api.particles.MetaParticle;
import nl.tno.sensorstorm.api.particles.Particle;
import nl.tno.sensorstorm.particlemapper.ParticleMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;

public abstract class SensorStormGrouping implements CustomStreamGrouping,
		Serializable {
	private static final long serialVersionUID = 9192870640924179453L;
	protected Logger logger = LoggerFactory.getLogger(this.getClass());

	private List<Integer> targetBoltIds;

	@Override
	public void prepare(WorkerTopologyContext context, GlobalStreamId stream,
			List<Integer> targetBoltIds) {
		this.targetBoltIds = targetBoltIds;
	}

	@Override
	public List<Integer> chooseTasks(int customGrouperTaskId,
			List<Object> values) {
		List<Integer> routeToBoltIds;

		// Is this tuple a Particle?
		Class<Particle> particleClass = valuesToParticleClass(values);
		if (particleClass != null) {
			// is this tuple a metaParticle?
			if (MetaParticle.class.isAssignableFrom(particleClass)) {
				routeToBoltIds = getBoltIdsForMetaParticle(targetBoltIds,
						particleClass, values);
			} else { // not a metaParticle particle
				routeToBoltIds = getBoltIdsForNonMetaParticle(targetBoltIds,
						particleClass, values);
			}
		} else {
			// else values can not be converted into a particle -> perform
			// default grouping
			routeToBoltIds = getBoltIdsForDefaultTuple(values);
		}

		if (particleClass == null) {
			System.out.println("Non particle routed to bolts ["
					+ routeToBoltIds + "]");
		} else {
			System.out.println("Particle (" + particleClass.getName()
					+ ") routed to bolts [" + routeToBoltIds + "]");
		}
		return routeToBoltIds;
	}

	@SuppressWarnings("unchecked")
	private Class<Particle> valuesToParticleClass(List<Object> values) {
		// values should at least contain 2 items, the timestamp and the
		// className
		if (values.size() >= ParticleMapper.PARTICLE_MINIMAL_FIELDS) {
			Object timestampObject = values.get(ParticleMapper.TIMESTAMP_IDX);
			Object particleClassObject = values
					.get(ParticleMapper.PARTICLE_CLASS_IDX);

			if ((timestampObject == null)
					|| (!(timestampObject instanceof Long))) {
				return null;
			}
			if ((particleClassObject == null)
					|| (!(particleClassObject instanceof String))) {
				return null;
			}

			try {
				String particleClassName = (String) particleClassObject;
				Class<?> particleClass = Class.forName(particleClassName);
				if (Particle.class.isAssignableFrom(particleClass)) {
					return (Class<Particle>) particleClass;
				} else {
					// Values particleClassName is not a particle
					return null;
				}
			} catch (ClassNotFoundException e) {
				// Values do not convert into a particle
				return null;
			}
		} else {
			return null;
		}
	}

	/**
	 * Return the boltIds to which this metaParticle must be routed.
	 * 
	 * @param targetBoltIdList
	 * @param particleClass
	 * @param values
	 * @return
	 */
	protected List<Integer> getBoltIdsForMetaParticle(
			List<Integer> targetBoltIdList,
			Class<? extends Particle> particleClass, List<Object> values) {
		List<Integer> boltIds = new ArrayList<Integer>();
		// broadcast the MetaParticle tuple
		boltIds.addAll(targetBoltIdList);
		return boltIds;
	}

	/**
	 * Get the boltId to which this nonMetaParticle must be routed. Default
	 * behaviour is to route on the first value == the timestamp van the
	 * particle, if available. Otherwise an empty list is returned.
	 * 
	 * @param targetBoltIdList
	 * @param particleClass
	 * @param values
	 * @return
	 */
	protected List<Integer> getBoltIdsForNonMetaParticle(
			List<Integer> targetBoltIdList,
			Class<? extends Particle> particleClass, List<Object> values) {

		// Is the timestamp field available to base routing on?
		if (ParticleMapper.TIMESTAMP_IDX < values.size()) {
			// route on the first value
			return getBoltIdsForValue(values.get(ParticleMapper.TIMESTAMP_IDX));
		} else {
			return new ArrayList<Integer>();
		}
	}

	/**
	 * Get the boltId to which this non particle tuple must be routed to. The
	 * routing will be based on the first value, if available. Otherwise an
	 * empty list is returned.
	 * 
	 * @param values
	 * @return
	 */
	protected List<Integer> getBoltIdsForDefaultTuple(List<Object> values) {
		// Is there at least one field available to base routing on?
		if (values.size() > 0) {
			// route on the first value
			return getBoltIdsForValue(values.get(0));
		} else {
			return new ArrayList<Integer>();
		}
	}

	/**
	 * Helper class to determine the boltId beloning to the value, useing the
	 * selectTargetBoltId method.
	 * 
	 * @param value
	 * @return
	 */
	protected List<Integer> getBoltIdsForValue(Object value) {
		List<Integer> boltIds = new ArrayList<Integer>();
		int targetBoltId = selectTargetBoltId(targetBoltIds, value);
		boltIds.add(targetBoltId);
		return boltIds;
	}

	/**
	 * Select the boltId from the targetBoltIdList, based on the value. Default
	 * behaviour is the hashcode of the value modulo the size of the
	 * targetBoltIdList
	 * 
	 * @param targetBoltIdList
	 * @param value
	 * @return
	 */
	protected int selectTargetBoltId(List<Integer> targetBoltIdList,
			Object value) {
		// object.hashCode can be negative! first abs before %.
		return targetBoltIdList.get(Math.abs(value.hashCode())
				% targetBoltIdList.size());
	}
}
