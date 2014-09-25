package nl.tno.timeseries.channels;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import nl.tno.storm.configuration.api.StormConfigurationException;
import nl.tno.storm.configuration.api.ZookeeperStormConfigurationAPI;
import nl.tno.storm.configuration.impl.ZookeeperStormConfigurationFactory;
import nl.tno.timeseries.annotation.FetcherDeclaration;
import nl.tno.timeseries.config.EmptyStormConfiguration;
import nl.tno.timeseries.interfaces.DataParticle;
import nl.tno.timeseries.interfaces.Fetcher;
import nl.tno.timeseries.interfaces.MetaParticle;
import nl.tno.timeseries.interfaces.Particle;
import nl.tno.timeseries.mapper.ParticleMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class ChannelSpout implements IRichSpout {

	private static final long serialVersionUID = -3199538353837853899L;

	public static final String SPOUT_TUPLECACHE_TIMEOUT_SEC = "spout.tuplecache.timeout";
	public static final String SPOUT_TUPLECACHE_MAX_SIZE = "spout.tuplecache.maxsize";

	protected Logger logger = LoggerFactory.getLogger(ChannelSpout.class);
	protected Cache<Object, Object> tupleCache;
	protected ZookeeperStormConfigurationAPI zookeeperStormConfiguration;
	protected SpoutOutputCollector collector;
	protected Fetcher fetcher;
	protected int nrOfOutputFields;

	/**
	 * Construct ChannelSpout. Subclasses are responsible for adding
	 * {@link MetaParticle}s to the config map! See ticket #3 for more elegant
	 * solution?.
	 * 
	 * @param config
	 * @param fetcher
	 */
	public ChannelSpout(Config config, Fetcher fetcher) {
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

		// initiate tuple cache if timeout is set
		if (stormNativeConfig.containsKey(SPOUT_TUPLECACHE_TIMEOUT_SEC)) {
			long timeout = ((Long) stormNativeConfig
					.get(SPOUT_TUPLECACHE_TIMEOUT_SEC)).intValue();
			int maxSize = ((Long) stormNativeConfig
					.get(SPOUT_TUPLECACHE_MAX_SIZE)).intValue();
			tupleCache = CacheBuilder.newBuilder().maximumSize(maxSize)
					.expireAfterAccess(timeout, TimeUnit.SECONDS).build();
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
		if (particle != null) {
			if (tupleCache != null) { // put particle in cache
				String msgId = particle.getChannelId() + "_"
						+ particle.getTimestamp();
				tupleCache.put(msgId, particle);
			}
			emitParticle(particle);
		}
	}

	protected void emitParticle(Particle particle) {
		collector.emit(ParticleMapper.particleToValues(particle,
				nrOfOutputFields));
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
	public void fail(Object msgId) {
		if (tupleCache != null && tupleCache.getIfPresent(msgId) != null) {
			emitParticle((Particle) tupleCache.getIfPresent(msgId));
		}
	}

	@Override
	public void ack(Object msgId) {
		if (tupleCache != null) {
			tupleCache.invalidate(msgId);
		}
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
