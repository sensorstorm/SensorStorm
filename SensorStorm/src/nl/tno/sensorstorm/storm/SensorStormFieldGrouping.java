package nl.tno.sensorstorm.storm;

import java.util.ArrayList;
import java.util.List;

import nl.tno.sensorstorm.api.particles.Particle;
import nl.tno.sensorstorm.particlemapper.ParticleMapper;

public class SensorStormFieldGrouping extends SensorStormGrouping {
	private static final long serialVersionUID = 2150546972712305082L;
	private final String fieldId;

	public SensorStormFieldGrouping(String fieldId) {
		this.fieldId = fieldId;
	}

	@Override
	protected List<Integer> getBoltIdsForNonMetaParticle(
			List<Integer> targetBoltIdList,
			Class<? extends Particle> particleClass, List<Object> values) {
		// get index of the fieldId in the values list
		List<Integer> boltIds = new ArrayList<Integer>();

		int fieldIdIndex = ParticleMapper.getFieldIdx(particleClass, fieldId);
		if ((fieldIdIndex >= 0) && (fieldIdIndex <= values.size())) {
			Object fieldValue = values.get(fieldIdIndex);
			int targetBoltId = selectTargetBoltId(targetBoltIdList, fieldValue);
			boltIds.add(targetBoltId);
		}
		return boltIds;
	}

}
