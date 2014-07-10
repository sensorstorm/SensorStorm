package nl.tno.timeseries.stormcomponents;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import nl.tno.timeseries.batchers.EmptyBatcher;
import nl.tno.timeseries.interfaces.Batcher;
import nl.tno.timeseries.interfaces.ChannelGrouper;
import nl.tno.timeseries.interfaces.EmitParticleInterface;
import nl.tno.timeseries.interfaces.Operation;
import nl.tno.timeseries.interfaces.Particle;
import nl.tno.timeseries.mapper.ParticleMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class ChannelBolt extends BaseRichBolt implements EmitParticleInterface {
	private static final long serialVersionUID = -5109656134961759532L;

	protected Logger logger = LoggerFactory.getLogger(ChannelBolt.class);
	protected @SuppressWarnings("rawtypes")Map stormConfig;
	protected OutputCollector collector;
	protected String boltName;
	protected Class<? extends Operation> operationClass;
	protected int nrOfOutputFields;
	protected Class<? extends Batcher> batcherClass;
	protected Map<String, ChannelManager> channelManagers;

	
	public ChannelBolt(Class<? extends Operation> operationClass, Class<? extends Batcher> batcherClass) {
		this.operationClass = operationClass;
		this.batcherClass = batcherClass;

		channelManagers = new HashMap<String, ChannelManager>();
	}

	public ChannelBolt(Class<? extends Operation> operationClass) {
		this(operationClass, EmptyBatcher.class);
	}
	
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, OutputCollector collector) {
		this.stormConfig = conf;
		this.collector = collector;
		this.boltName = context.getThisComponentId();
	}
	

	@Override
	public void execute(Tuple tuple) {
		Particle inputParticle = ParticleMapper.tupleToParticle(tuple);
		if (inputParticle != null) {
			String selectChannelManagerId = inputParticle.getChannelId();
			// determine if there was a channelGrouper in front of this bolt, if so use the channelGroup as grouper.
			String channelGroupId;
			try { 
				channelGroupId = tuple.getStringByField(ChannelGrouper.GROUPED_PARTICLE_FIELD);
			} catch (IllegalArgumentException e) {
				channelGroupId = null;
			}
			if (channelGroupId != null) {
				selectChannelManagerId = channelGroupId;
			}
			
			ChannelManager channelManager = getChannelManager(selectChannelManagerId);
			List<Particle> outputParticles = channelManager.processParticle(inputParticle);
			if (outputParticles != null) {
				for (Particle outputParticle : outputParticles) {
					emitParticle(outputParticle);
				}
			}
		}
	}

	
	private ChannelManager getChannelManager(String channelId) {
		ChannelManager channelManager = channelManagers.get(channelId);
		if (channelManager == null) {
			channelManager = new ChannelManager(channelId, batcherClass, operationClass, stormConfig, this);
			channelManagers.put(channelId, channelManager);
		}
		return channelManager;
	}
	
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// merge all output particle fields and meta particle fields
		Fields fields = null;
		List<Class<? extends Particle>> outputParticles = ChannelManager.getOutputParticles(operationClass);
		for (Class<? extends Particle> outputParticleClass: outputParticles) {
			if (fields == null) {
				fields = ParticleMapper.getFields(outputParticleClass);
			} else {
				fields = ParticleMapper.mergeFields(fields, ParticleMapper.getFields(outputParticleClass));
			}
		}
		
		nrOfOutputFields = fields.size();
		declarer.declare(fields);
	}


	@Override
	public void emitParticle(Particle particle) {
		collector.emit(ParticleMapper.particleToValues(particle, nrOfOutputFields));
	}
	
}
