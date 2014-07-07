package nl.tno.timeseries.stormcomponents;

import java.util.Map;

import nl.tno.timeseries.interfaces.DataParticle;
import nl.tno.timeseries.interfaces.Fetcher;
import nl.tno.timeseries.interfaces.Particle;
import nl.tno.timeseries.mapper.ParticleMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

public class ChannelSpout implements IRichSpout {
	private static final long serialVersionUID = -3199538353837853899L;

	protected Logger logger = LoggerFactory.getLogger(ChannelSpout.class);
	
	protected SpoutOutputCollector collector;
	protected Fetcher fetcher;
	protected Class<? extends Particle> outputParticleClass;

	
	public ChannelSpout(Fetcher fetcher, Class<? extends Particle> outputParticleClass) {
		this.fetcher = fetcher;
		this.outputParticleClass = outputParticleClass;
	}

	
	@Override
	public void open(@SuppressWarnings("rawtypes")Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		
		try {
			fetcher.prepare(conf, context);
		} catch (Exception e) {
			logger.warn("Unable to configure channelSpout due to ", e);
		}
	}

	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		Fields fields = ParticleMapper.getFields(outputParticleClass);
		//@TODO merge fields with MetaParticle fields
		declarer.declare(fields);
	}
	

	@Override
	public void nextTuple() {
		DataParticle particle = fetcher.fetchParticle();
		if (particle != null) {
			collector.emit(ParticleMapper.particleToValues(particle));
		}
	}
	
	

	@Override
	public void activate() {
		fetcher.activate();
	}

	@Override
	public void close() {
		fetcher.deactivate();
	}

	@Override
	public void deactivate() {
		fetcher.deactivate();
	}

	
	
	@Override
	public void fail(Object arg0) {
		
	}
	@Override
	public void ack(Object arg0) {
	}



	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
