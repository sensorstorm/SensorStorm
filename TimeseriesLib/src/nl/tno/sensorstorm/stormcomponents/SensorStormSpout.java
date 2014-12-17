package nl.tno.sensorstorm.stormcomponents;

import java.util.Map;

import nl.tno.sensorstorm.annotation.FetcherDeclaration;
import nl.tno.sensorstorm.config.EmptyStormConfiguration;
import nl.tno.sensorstorm.fetchers.Fetcher;
import nl.tno.sensorstorm.mapper.ParticleMapper;
import nl.tno.sensorstorm.particles.DataParticle;
import nl.tno.sensorstorm.particles.MetaParticle;
import nl.tno.sensorstorm.particles.MetaParticleUtil;
import nl.tno.sensorstorm.particles.Particle;
import nl.tno.sensorstorm.particles.timer.TimerTickParticle;
import nl.tno.storm.configuration.api.ExternalStormConfiguration;
import nl.tno.storm.configuration.api.StormConfigurationException;
import nl.tno.storm.configuration.impl.ZookeeperStormConfigurationFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

/**
 * @TODO add general sensorspout explanation
 * 
 *       This spout adds main timer functionality to the SensorStormSpout. The
 *       main timer can be synced to the live timer of the server the spout is
 *       running on or to the time that is used in the DataParticles being
 *       produced by the fetcher. The main timer is sued to insert
 *       TimerTickParticles in each Channel this Spout is producing.
 * 
 *       In order to be able to sync on the DataParticle timestamp , timertick
 *       particles are inserted only just before the DataParticle is inserted.
 *       they come in batches.
 * 
 *       If the main timer is synced to the live timer and it is time in
 *       relation to the mainTimerTickFreq, timerticks are emitted on all
 *       already discovered channels.
 * 
 *       It is up to the fetchers to create a synced time over all sources. If
 *       there are two or more sources, each with their own timestamp, it is up
 *       to the fetchers to determine a combined timestamp over all sources. The
 *       TimerSensorStormSpout deals with only one timestamp. Also over
 *       SensorStormSpouts of different type, it is up to the fetchers to create
 *       a combined timestamp.
 * 
 * @author waaijbdvd
 * 
 */

public class SensorStormSpout implements IRichSpout {
	private static final long serialVersionUID = -3199538353837853899L;

	protected Logger logger = LoggerFactory.getLogger(SensorStormSpout.class);
	protected ExternalStormConfiguration zookeeperStormConfiguration;
	protected SpoutOutputCollector collector;
	protected Fetcher fetcher;
	protected int nrOfOutputFields;
	private Long mainTimerTickFreq;
	private Boolean useParticleTime;
	private Long lastKnownNow;

	/**
	 * Construct SensorStormSpout. Subclasses are responsible for adding
	 * {@link MetaParticle}s to the config map! See ticket #3 for more elegant
	 * solution?.
	 * 
	 * Default is that the main timer will be synced to the incoming particles,
	 * but the mainTimerTickFreq is set to 0 which means no TimerTickParticles
	 * will be produced.
	 * 
	 * @param config
	 *            Reference to the Storm config.
	 * @param fetcher
	 *            Reference to the fetcher instance to be used.
	 */
	public SensorStormSpout(Config config, Fetcher fetcher) {
		sensorStormSpout(fetcher, true, 0L, config);
	}

	/**
	 * Create a new TimerSensorStormSpout. How to sync the main timer and what
	 * its frequency is can be specified.
	 * 
	 * @param config
	 *            Reference to the Storm config.
	 * @param fetcher
	 *            Reference to the fetcher instance to be used.
	 * @param useParticleTime
	 *            Parameter to indicate how to sync the main timer. True means
	 *            it is synced to the time in the DataParticle coming from the
	 *            fetcher. False means it is synced to the live timer of the
	 *            server this spout is running on.
	 * @param mainTimerTickFreq
	 *            The frequency the main timer must run on in ms.
	 */
	public SensorStormSpout(Config config, Fetcher fetcher,
			Boolean useParticleTime, Long mainTimerTickFreq) {
		sensorStormSpout(fetcher, useParticleTime, mainTimerTickFreq, config);
	}

	/**
	 * Common constructor code.
	 * 
	 * @param config
	 * @param fetcher
	 * @param useParticleTime
	 * @param mainTimerTickFreq
	 */
	private void sensorStormSpout(Fetcher fetcher, Boolean useParticleTime,
			Long mainTimerTickFreq, Config config) {
		this.fetcher = fetcher;
		this.mainTimerTickFreq = mainTimerTickFreq;
		this.useParticleTime = useParticleTime;
		lastKnownNow = null;

		MetaParticleUtil.registerMetaParticleFieldsWithMetaParticleClass(
				config, TimerTickParticle.class);
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

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		Fields outputFields = getOutputFields();
		nrOfOutputFields = outputFields.size();
		declarer.declare(outputFields);
	}

	/**
	 * Fetches a new DataParticle from the fetcher. Syncs the main timer. Emits
	 * zero or more TimerTickParticles. Emits the dataParticle.
	 */
	@Override
	public void nextTuple() {
		// get next particle
		DataParticle particle = fetcher.fetchParticle();

		// emit particle together with optional leading timerTicks
		if (particle != null) {
			Long now;
			if (useParticleTime) {
				now = particle.getTimestamp();
			} else {
				now = System.currentTimeMillis();
			}

			emitTimerTicks(now);
			emitParticle(particle);
		}

		// emit always realtime timerTicks if necessary, also if there is no
		// particle to emit
		if (!useParticleTime) {
			emitTimerTicks(System.currentTimeMillis());
		}
	}

	/**
	 * Emit timerTicks from lastKnowNow up to now. Emit timerTicks up to and
	 * including now
	 * 
	 * @param now
	 */
	private void emitTimerTicks(long now) {
		// Do we have to emit timerTicks?
		if (mainTimerTickFreq != 0) {
			// firstTime? start from now
			if (lastKnownNow == null) {
				lastKnownNow = now;
				// emit first timerTick at the same time as the particle
				emitParticle(new TimerTickParticle(now));
			} else {
				// emit zero or more timerTicks up to now
				while (now - lastKnownNow >= mainTimerTickFreq) {
					lastKnownNow = lastKnownNow + mainTimerTickFreq;
					emitParticle(new TimerTickParticle(lastKnownNow));
				}
			}
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

		// add TimerTickParticle fields
		fields = ParticleMapper.mergeFields(fields,
				ParticleMapper.getFields(TimerTickParticle.class));

		return fields;
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
