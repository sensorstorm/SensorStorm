package nl.tno.timeseries.stormcomponents;

import java.util.Map;

import nl.tno.timeseries.annotation.FetcherDeclaration;
import nl.tno.timeseries.interfaces.DataParticle;
import nl.tno.timeseries.interfaces.Fetcher;
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
	protected int nrOfOutputFields;

	
	public ChannelSpout(Fetcher fetcher) {
		this.fetcher = fetcher;
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
	
	protected Fields getOutputFields() {
		Fields fields = null;
		FetcherDeclaration fetcherDeclaration = fetcher.getClass().getAnnotation(FetcherDeclaration.class);
		for (Class<? extends DataParticle> outputParticleClass : fetcherDeclaration.outputs()) {
			if (fields == null) {
				fields = ParticleMapper.getFields(outputParticleClass);
			} else {
				fields = ParticleMapper.mergeFields(fields, ParticleMapper.getFields(outputParticleClass));
			}
		}
		
		return fields;
	}
	
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		Fields outputFields = getOutputFields();
		nrOfOutputFields = outputFields.size();
		declarer.declare(outputFields);
	}
	

	@Override
	public void nextTuple() {
		DataParticle particle = fetcher.fetchParticle();
		if (particle != null) {
			collector.emit(ParticleMapper.particleToValues(particle, nrOfOutputFields));
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
