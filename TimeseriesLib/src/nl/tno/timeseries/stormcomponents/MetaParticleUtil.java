package nl.tno.timeseries.stormcomponents;

import java.util.List;

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

	public static Fields setMetaParticleFields(Config conf, Class<? extends Operation> operationClass) {
		Fields res = null;
		if (conf.containsKey(METADATA_FIELDS)) {
			@SuppressWarnings("unchecked")
			Fields old = new Fields((List<String>) conf.get(METADATA_FIELDS));
			Fields toAdd = getMetaParticleOutputFields(operationClass);
			res = ParticleMapper.mergeFields(old, toAdd);
		} else {
			res = getMetaParticleOutputFields(operationClass);
		}
		conf.put(METADATA_FIELDS, res.toList());
		return res;
	}

	public static Fields setMetaParticleFieldsWithParticle(Config conf, Class<? extends MetaParticle> metaParticleClass) {
		Fields res = null;
		if (conf.containsKey(METADATA_FIELDS)) {
			@SuppressWarnings("unchecked")
			Fields old = new Fields((List<String>) conf.get(METADATA_FIELDS));
			Fields toAdd = ParticleMapper.getFields(metaParticleClass);
			res = ParticleMapper.mergeFields(old, toAdd);
		} else {
			res = ParticleMapper.getFields(metaParticleClass);
		}
		conf.put(METADATA_FIELDS, res.toList());
		return res;
	}

	public static Fields getMetaParticleOutputFields(Class<? extends Operation> operationClass) {
		List<Class<? extends MetaParticle>> outputMetaParticles = ChannelManager.getOutputMetaParticles(operationClass);
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
