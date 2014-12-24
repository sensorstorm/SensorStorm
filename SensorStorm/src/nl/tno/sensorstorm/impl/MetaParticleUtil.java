package nl.tno.sensorstorm.impl;

import java.util.List;

import nl.tno.sensorstorm.api.particles.MetaParticle;
import nl.tno.sensorstorm.api.processing.Operation;
import nl.tno.sensorstorm.particlemapper.ParticleMapper;
import backtype.storm.Config;
import backtype.storm.tuple.Fields;

// TODO explain this class
public class MetaParticleUtil {

	public static final String METADATA_FIELDS = "metadata.fields";

	@SuppressWarnings("unchecked")
	public static Fields getMetaParticleFields(Config conf) {
		if (conf.containsKey(METADATA_FIELDS)) {
			return new Fields((List<String>) conf.get(METADATA_FIELDS));
		} else {
			return new Fields(new String[0]);
		}
	}

	/**
	 * This one is for the bolts
	 */
	@SuppressWarnings("unchecked")
	public static Fields registerMetaParticleFieldsFromOperationClass(
			Config conf, Class<? extends Operation> operationClass) {
		Fields metaParticleFields = null;
		if (conf.containsKey(METADATA_FIELDS)) {
			metaParticleFields = new Fields(
					(List<String>) conf.get(METADATA_FIELDS));
		}
		Fields fields = ParticleMapper.mergeFields(metaParticleFields,
				getMetaParticleOutputFieldsFromOperationClass(operationClass));
		conf.put(METADATA_FIELDS, fields.toList());

		return fields;
	}

	/**
	 * This one is for the spout
	 */
	@SuppressWarnings("unchecked")
	public static Fields registerMetaParticleFieldsFromMetaParticleClass(
			Config conf, Class<? extends MetaParticle> metaParticleClass) {
		Fields metaParticleFields = null;
		if (conf.containsKey(METADATA_FIELDS)) {
			metaParticleFields = new Fields(
					(List<String>) conf.get(METADATA_FIELDS));
		}
		Fields fields = ParticleMapper.mergeFields(metaParticleFields,
				ParticleMapper.getFields(metaParticleClass));
		conf.put(METADATA_FIELDS, fields.toList());

		return fields;
	}

	public static Fields getMetaParticleOutputFieldsFromOperationClass(
			Class<? extends Operation> operationClass) {
		List<Class<? extends MetaParticle>> outputMetaParticles = OperationManager
				.getOutputMetaParticles(operationClass);
		Fields fields = null;
		for (Class<? extends MetaParticle> p : outputMetaParticles) {
			Fields newfields = ParticleMapper.getFields(p);
			if (fields == null) {
				fields = newfields;
			} else {
				fields = ParticleMapper.mergeFields(fields, newfields);
			}
		}
		return fields;
	}

}
