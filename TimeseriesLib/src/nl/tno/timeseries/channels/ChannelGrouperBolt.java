package nl.tno.timeseries.channels;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import nl.tno.timeseries.annotation.ChannelGrouperDeclaration;
import nl.tno.timeseries.interfaces.ChannelGrouper;
import nl.tno.timeseries.interfaces.DataParticle;
import nl.tno.timeseries.interfaces.MetaParticle;
import nl.tno.timeseries.interfaces.Particle;
import nl.tno.timeseries.mapper.ParticleMapper;
import nl.tno.timeseries.particles.MetaParticleUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class ChannelGrouperBolt extends BaseRichBolt {
	private static final long serialVersionUID = -4421389584087020459L;
	protected Logger logger = LoggerFactory.getLogger(ChannelGrouperBolt.class);
	protected @SuppressWarnings("rawtypes")
	Map stormConfig;
	protected OutputCollector collector;
	protected String boltName;
	protected int nrOfOutputFields;
	protected List<String> channelGroupIds;
	protected ChannelGrouper channelGrouper;
	private final Fields metaParticleFields;

	public ChannelGrouperBolt(Config conf, ChannelGrouper grouper) {
		this.channelGrouper = grouper;
		channelGroupIds = new ArrayList<String>();
		this.metaParticleFields = MetaParticleUtil.getMetaParticleFields(conf);
	}

	@Override
	public void execute(Tuple tuple) {
		Particle inputParticle = ParticleMapper.tupleToParticle(tuple);

		if (inputParticle instanceof DataParticle) {
			handleDataParticle((DataParticle) inputParticle);
		} else if (inputParticle instanceof MetaParticle) {
			handleMetaParticle((MetaParticle) inputParticle);
		}
	}

	protected void handleDataParticle(DataParticle dataParticle) {
		List<String> channelGroupIds = channelGrouper
				.getChannelGroupId(dataParticle.getChannelId());
		for (String channelGroupId : channelGroupIds) {
			if (!channelGroupIds.contains(channelGroupId)) {
				channelGroupIds.add(channelGroupId);
			}
			emitParticle(dataParticle, channelGroupId);
		}
	}

	protected void handleMetaParticle(MetaParticle metaParticle) {
		// copy all metaParticles to the group channels
		for (String channelGroupId : channelGroupIds) {
			metaParticle.setChannelId(channelGroupId);
			emitParticle(metaParticle, channelGroupId);
		}
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf,
			TopologyContext context, OutputCollector collector) {
		this.stormConfig = conf;
		this.collector = collector;
		this.boltName = context.getThisComponentId();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		Fields fields = null;
		ChannelGrouperDeclaration channelGrouperDeclaration = channelGrouper
				.getClass().getAnnotation(ChannelGrouperDeclaration.class);
		for (Class<? extends DataParticle> outputParticleClass : channelGrouperDeclaration
				.outputs()) {
			if (fields == null) {
				fields = ParticleMapper.getFields(outputParticleClass);
			} else {
				fields = ParticleMapper.mergeFields(fields,
						ParticleMapper.getFields(outputParticleClass));
			}
		}

		fields = ParticleMapper.mergeFields(fields, new Fields(
				ChannelGrouper.GROUPED_PARTICLE_FIELD));
		fields = ParticleMapper.mergeFields(fields, metaParticleFields);
		nrOfOutputFields = fields.size();
		declarer.declare(fields);

		// String s = "[";
		// for (String field : fields) {
		// s = s + field + ", ";
		// }
		// s = s + "]";
		//
		// System.out.println("group.declareOutputFields.nrOfOutputFields="+nrOfOutputFields+" fields="+s);
	}

	public void emitParticle(Particle particle, String channelGroupId) {
		// convert the particle to values without the channelGroupId field
		Values values = ParticleMapper.particleToValues(particle,
				nrOfOutputFields - 1);
		values.add(channelGroupId);
		collector.emit(values);

		// String s = "[";
		// for (Object value : values) {
		// if (value == null) {
		// s = s + "null, ";
		// } else {
		// s = s + value.toString() + ", ";
		// }
		// }
		// s = s + "]";
		// System.out.println("group.emitParticle("+values.size()+") values="+s);
	}

}
