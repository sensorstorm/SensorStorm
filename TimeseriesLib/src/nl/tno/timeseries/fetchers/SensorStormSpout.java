package nl.tno.timeseries.fetchers;

import java.util.Map;

import nl.tno.storm.configuration.api.ExternalStormConfiguration;
import nl.tno.storm.configuration.api.StormConfigurationException;
import nl.tno.storm.configuration.impl.ZookeeperStormConfigurationFactory;
import nl.tno.timeseries.annotation.FetcherDeclaration;
import nl.tno.timeseries.config.EmptyStormConfiguration;
import nl.tno.timeseries.mapper.ParticleMapper;
import nl.tno.timeseries.particles.DataParticle;
import nl.tno.timeseries.particles.MetaParticle;
import nl.tno.timeseries.particles.Particle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

public class SensorStormSpout implements IRichSpout {
	private static final long serialVersionUID = -3199538353837853899L;

	protected Logger logger = LoggerFactory.getLogger(SensorStormSpout.class);
	protected ExternalStormConfiguration zookeeperStormConfiguration;
	protected SpoutOutputCollector collector;
	protected Fetcher fetcher;
	protected int nrOfOutputFields;

	/**
	 * Construct SensorStormSpout. Subclasses are responsible for adding
	 * {@link MetaParticle}s to the config map! See ticket #3 for more elegant
	 * solution?.
	 * 
	 * @param config
	 * @param fetcher
	 */
	public SensorStormSpout(Config config, Fetcher fetcher) {
		this.fetcher = fetcher;
	}

	@Override
	public void open(@SuppressWarnings("rawtypes") Map stormNativeConfig,
			TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;

		try {
			zookeeperStormConfiguration = ZookeeperStormConfigurationFactory
					.getInstance().getStormConfiguration(stormNativeConfig);
		} catch (StormConfigurationException e) {
			logger.error("Can not connect to zookeeper for get Storm configuration. Reason: "
					+ e.getMessage());
			zookeeperStormConfiguration = new EmptyStormConfiguration();
		}

		try {
			fetcher.prepare(stormNativeConfig, zookeeperStormConfiguration,
					context);
		} catch (Exception e) {
			logger.warn("Unable to configure channelSpout "
					+ this.getClass().getName() + " due to ", e);
		}
	}

	protected Fields getOutputFields() {
		// TODO Add also all metaparticles in the outputFields list
		Fields fields = null;
		FetcherDeclaration fetcherDeclaration = fetcher.getClass()
				.getAnnotation(FetcherDeclaration.class);
		for (Class<? extends DataParticle> outputParticleClass : fetcherDeclaration
				.outputs()) {
			if (fields == null) {
				fields = ParticleMapper.getFields(outputParticleClass);
			} else {
				fields = ParticleMapper.mergeFields(fields,
						ParticleMapper.getFields(outputParticleClass));
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
		emitParticle(particle);
	}

	/**
	 * Emit a particle, both DataParticle and MetaParticle are possible
	 * 
	 * @param particle
	 */
	public void emitParticle(Particle particle) {
		if (particle != null) {
			collector.emit(ParticleMapper.particleToValues(particle,
					nrOfOutputFields));
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
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	@Override
	public void ack(Object msgId) {
		// no retransmit is supported, only throttling
	}

	@Override
	public void fail(Object msgId) {
		// no retransmit is supported, only throttling
	}

}
