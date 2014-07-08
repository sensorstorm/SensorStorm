package nl.tno.timeseries.stormcomponents;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import nl.tno.timeseries.interfaces.DataParticle;
import nl.tno.timeseries.interfaces.GroupedParticle;
import nl.tno.timeseries.interfaces.MetaParticle;
import nl.tno.timeseries.interfaces.Particle;
import nl.tno.timeseries.mapper.ParticleMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class ChannelGrouperBolt extends BaseRichBolt {
	private static final long serialVersionUID = -4421389584087020459L;
	protected Logger logger = LoggerFactory.getLogger(ChannelGrouperBolt.class);
	protected @SuppressWarnings("rawtypes")Map stormConfig;
	protected OutputCollector collector;
	protected String boltName;
	protected int nrOfOutputFields;
	protected Map<String, Set<String>> channelGroups;

	
	public ChannelGrouperBolt() {
		channelGroups = new HashMap<String, Set<String>>();
		
		//TODO get config from external
		Set<String> group1 = new HashSet<String>();
		group1.add("S1");
		group1.add("S2");
		Set<String> group2 = new HashSet<String>();
		group2.add("S2");
		group2.add("S3");
		
		channelGroups.put("G1", group1);
		channelGroups.put("G2", group2);
	}
	

	@Override
	public void execute(Tuple tuple) {
		Particle inputParticle = ParticleMapper.tupleToParticle(tuple);

		if (inputParticle instanceof DataParticle) {
			groupDataParticle((DataParticle)inputParticle);
		} else if (inputParticle instanceof MetaParticle) {
			// copy all metaParticles to the group channels
			for (String groupId : channelGroups.keySet()) {
				inputParticle.setChannelId(groupId);
				emitParticle(inputParticle);
			}
		}
	}

	
	protected void groupDataParticle(DataParticle inputParticle) {
	}


	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, OutputCollector collector) {
		this.stormConfig = conf;
		this.collector = collector;
		this.boltName = context.getThisComponentId();
	}

	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
	}

	
	public void emitParticle(Particle particle) {
		collector.emit(ParticleMapper.particleToValues(particle, nrOfOutputFields));
	}

}
