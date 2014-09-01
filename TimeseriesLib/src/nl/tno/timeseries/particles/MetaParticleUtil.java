package nl.tno.timeseries.particles;

import java.util.List;

import nl.tno.timeseries.channels.ChannelManager;
import nl.tno.timeseries.interfaces.MetaParticle;
import nl.tno.timeseries.interfaces.Operation;
import nl.tno.timeseries.mapper.ParticleMapper;
import backtype.storm.Config;
import backtype.storm.tuple.Fields;

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
	public static Fields registerMetaParticleFieldsWithOperationClass(
			Config conf, Class<? extends Operation> operationClass) {
		Fields metaParticleFields = null;
		if (conf.containsKey(METADATA_FIELDS)) {
			metaParticleFields = new Fields(
					(List<String>) conf.get(METADATA_FIELDS));
		}
		Fields fields = ParticleMapper.mergeFields(metaParticleFields,
				getMetaParticleOutputFields(operationClass));
		conf.put(METADATA_FIELDS, fields.toList());

		return fields;
	}

	/**
	 * This one is for the spout
	 */
	@SuppressWarnings("unchecked")
	public static Fields registerMetaParticleFieldsWithMetaParticleClass(
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

	public static Fields getMetaParticleOutputFields(
			Class<? extends Operation> operationClass) {
		List<Class<? extends MetaParticle>> outputMetaParticles = ChannelManager
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
