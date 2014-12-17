package nl.tno.sensorstorm.stormcomponents.groupers;

import java.util.ArrayList;
import java.util.List;

import nl.tno.sensorstorm.mapper.ParticleMapper;
import nl.tno.sensorstorm.particles.Particle;

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
			System.out.println("  selected bolt " + targetBoltId
					+ " based on field " + fieldId + "(" + fieldValue + ")");
			boltIds.add(targetBoltId);
		}
		return boltIds;
	}

}
