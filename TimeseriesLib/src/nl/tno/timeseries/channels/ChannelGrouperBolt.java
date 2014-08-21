package nl.tno.timeseries.channels;

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

/**
 * This Bolt can group channels (meaning all particles in that channel) into one
 * or more new channels. The precise mapping is performed by a ChannelGrouper
 * object, passed in the constructor. This mapping may contain duplicates: the
 * same particle can be grouped into several channels. Grouped channels transmit
 * Particles with the field ChannelGrouper.GROUPED_PARTICLE_FIELD to indicate
 * this is a grouped particle.
 * 
 * @author waaijbdvd
 * 
 */
public class ChannelGrouperBolt extends BaseRichBolt {
	private static final long serialVersionUID = -4421389584087020459L;
	private final Logger logger = LoggerFactory
			.getLogger(ChannelGrouperBolt.class);

	private OutputCollector collector;
	private String boltName;
	private int nrOfOutputFields;
	private final ChannelGrouper channelGrouper;
	private final Fields metaParticleFields;

	/**
	 * A ChannelGrouperBolt groups channels into new channels, , based on the
	 * given ChannelGrouper.
	 * 
	 * @param conf
	 * @param grouper
	 */
	public ChannelGrouperBolt(Config conf, ChannelGrouper grouper) {
		this.channelGrouper = grouper;
		this.metaParticleFields = MetaParticleUtil.getMetaParticleFields(conf);
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf,
			TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.boltName = context.getThisComponentId();
	}

	/**
	 * A new tuple has arrived. Convert the tuple into a particle and call
	 * handleDataParticle or handleMetaParticle.
	 */
	@Override
	public void execute(Tuple tuple) {
		Particle inputParticle = ParticleMapper.tupleToParticle(tuple);

		if (inputParticle instanceof DataParticle) {
			handleDataParticle((DataParticle) inputParticle);
		} else if (inputParticle instanceof MetaParticle) {
			handleMetaParticle((MetaParticle) inputParticle);
		}
	}

	/**
	 * Handle a dataParticle: ask the channelGrouper for a list of channels this
	 * particle must be send to.
	 * 
	 * @param dataParticle
	 */
	protected void handleDataParticle(DataParticle dataParticle) {
		List<String> channelGroupIds = channelGrouper
				.getChannelGroupIds(dataParticle.getChannelId());
		for (String channelGroupId : channelGroupIds) {
			emitParticle(dataParticle, channelGroupId);
		}
	}

	/**
	 * Handle a metaParticle: send the metaParticle to all possible group
	 * channels.
	 * 
	 * @param metaParticle
	 */
	protected void handleMetaParticle(MetaParticle metaParticle) {
		List<String> allChannelGroupIds = channelGrouper
				.getAllChannelGroupIds();
		for (String channelGroupId : allChannelGroupIds) {
			emitParticle(metaParticle, channelGroupId);
		}
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

		fields = ParticleMapper.mergeFields(fields, metaParticleFields);

		// be aware that this has to be the last field, otherwise the
		// emitParticle method can not add the channelGroupId.
		fields = ParticleMapper.mergeFields(fields, new Fields(
				ChannelGrouper.GROUPED_PARTICLE_FIELD));
		nrOfOutputFields = fields.size();
		declarer.declare(fields);
	}

	/**
	 * Emit a particle to the specified channelGroupId.
	 * 
	 * @param particle
	 * @param channelGroupId
	 */
	private void emitParticle(Particle particle, String channelGroupId) {
		if (particleAllowed(particle)) {
			// convert the particle to values without the channelGroupId field
			Values values = ParticleMapper.particleToValues(particle,
					nrOfOutputFields - 1);
			values.add(channelGroupId);
			collector.emit(values);
		} else {
			logger.debug(boltName
					+ ": Unspecified particle ("
					+ particle.getClass().getName()
					+ ") is not passed, it is not specified in the ChannelGrouperDeclaration, the annotation belonging to a ChannelGrouper.");
		}
	}

	/**
	 * Cheks if the particle is a metaParticle or a member of the
	 * ChannelGrouperDeclaration annotation output list.
	 * 
	 * @param particle
	 * @return
	 */
	protected boolean particleAllowed(Particle particle) {
		if (particle instanceof MetaParticle) {
			return true;
		}
		ChannelGrouperDeclaration channelGrouperDeclaration = channelGrouper
				.getClass().getAnnotation(ChannelGrouperDeclaration.class);
		for (Class<? extends DataParticle> outputParticleClass : channelGrouperDeclaration
				.outputs()) {
			if (particle.getClass().isAssignableFrom(outputParticleClass)) {
				return true;
			}
		}
		return false;
	}
}
