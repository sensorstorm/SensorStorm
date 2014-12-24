package nl.tno.sensorstorm.storm;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import nl.tno.sensorstorm.api.annotation.FetcherDeclaration;
import nl.tno.sensorstorm.api.particles.DataParticle;
import nl.tno.sensorstorm.api.particles.MetaParticle;
import nl.tno.sensorstorm.api.particles.Particle;
import nl.tno.sensorstorm.api.processing.Fetcher;
import nl.tno.sensorstorm.api.processing.Operation;
import nl.tno.sensorstorm.config.EmptyStormConfiguration;
import nl.tno.sensorstorm.impl.MetaParticleUtil;
import nl.tno.sensorstorm.particlemapper.ParticleMapper;
import nl.tno.sensorstorm.timer.TimerTickParticle;
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
 * This is a generic Spout for the SensorStorm library. The logic for retrieving
 * data is implemented in a {@link Fetcher}, which runs on top of this spout.
 * 
 * This Spout injects {@link TimerTickParticle}s into the stream. This way,
 * {@link Operation}s in the topology are able to perform scheduled tasks and
 * recurring, even if there is no data to trigger processing. There are two ways
 * of doing this: Using the timestamps as they are set in the {@link Particle}
 * produced by the {@link Fetcher}, of using the system time. When the system
 * time is used, the timestamps in the particles will be overwritten before the
 * are sent to the topology. In both cases measures must be taken to make sure
 * that the timestamps in Particles are synchronized over all instances of
 * {@link SensorStormSpout}s.
 * 
 * In order to implement specific behavior (usually sending out more types of
 * {@link MetaParticle}s) this class can be extended. When a subclass introduces
 * new types of {@link MetaParticle}s, the subclass must call the
 * registerMetaParticle method in the constructor, so the topology will be able
 * to process the new type of {@link MetaParticle}.
 */
public class SensorStormSpout implements IRichSpout {

	private static final long serialVersionUID = -3199538353837853899L;

	protected Logger logger = LoggerFactory.getLogger(SensorStormSpout.class);
	protected ExternalStormConfiguration zookeeperStormConfiguration;
	protected SpoutOutputCollector collector;
	protected Fetcher fetcher;
	protected int nrOfOutputFields;
	private final long mainTimerTickFreqMs;
	private final boolean useParticleTime;
	private long lastKnownNow;
	private final List<Class<? extends MetaParticle>> registeredMetaParticles;
	private String originId;

	/**
	 * Construct a SensorStormSpout with a {@link Fetcher} and no Timer
	 * functionality.
	 * 
	 * Default is that the main timer will be synced to the incoming particles,
	 * but the mainTimerTickFreq is set to 0 which means no TimerTickParticles
	 * will be produced.
	 * 
	 * @param config
	 *            Reference to the native Storm config.
	 * @param fetcher
	 *            Reference to the fetcher instance to be used.
	 * @throws IllegalArgumentException
	 *             when the {@link Fetcher} does not have a
	 *             {@link FetcherDeclaration} annotation
	 */
	public SensorStormSpout(Config config, Fetcher fetcher) {
		this(config, fetcher, true, 0);
	}

	/**
	 * Construct a SensorStormSpout with a {@link Fetcher} and Timer
	 * functionality.
	 * 
	 * @param config
	 *            Reference to the Storm config.
	 * @param fetcher
	 *            Reference to the fetcher instance to be used.
	 * @param useParticleTime
	 *            Parameter to indicate how to synchronize the main timer. True
	 *            means it is synchronized to the time in the
	 *            {@link DataParticle} coming from the {@link Fetcher}. False
	 *            means it is synchronized to the system clock of the server
	 *            this spout is running on AND that the timestap in the
	 *            {@link Particle}s will be overwritten with the system time
	 * @param mainTimerTickFreqMs
	 *            The frequency the main timer must run on in milliseconds.
	 * @throws IllegalArgumentException
	 *             when the {@link Fetcher} does not have a
	 *             {@link FetcherDeclaration} annotation
	 */
	public SensorStormSpout(Config config, Fetcher fetcher,
			boolean useParticleTime, long mainTimerTickFreqMs) {
		this.fetcher = fetcher;
		if (fetcher.getClass().getAnnotation(FetcherDeclaration.class) == null) {
			throw new IllegalArgumentException("The fetcher "
					+ fetcher.getClass().getName()
					+ " is missing the FetcherDeclaration annotation");
		}
		this.mainTimerTickFreqMs = mainTimerTickFreqMs;
		this.useParticleTime = useParticleTime;
		lastKnownNow = -1;
		registeredMetaParticles = new ArrayList<Class<? extends MetaParticle>>();
		registerMetaParticle(config, TimerTickParticle.class);
	}

	/**
	 * Register a new type of {@link MetaParticle} so it can be processed by the
	 * Storm topology. Subclasses of {@link SensorStormSpout} should call this
	 * method in the constructor if they introduced new types of
	 * {@link MetaParticle}s.
	 * 
	 * @param config
	 *            The native storm configuration
	 * @param metaParticleClass
	 *            Class of the {@link MetaParticle} that is produces by this
	 *            spout
	 */
	protected void registerMetaParticle(Config config,
			Class<? extends MetaParticle> metaParticleClass) {
		MetaParticleUtil.registerMetaParticleFieldsFromMetaParticleClass(
				config, TimerTickParticle.class);
		registeredMetaParticles.add(metaParticleClass);
	}

	@Override
	public void open(@SuppressWarnings("rawtypes") Map stormNativeConfig,
			TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		originId = this.getClass().getName() + "." + context.getThisTaskIndex();

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
			logger.warn("Unable to configure SensorStormSpout "
					+ this.getClass().getName() + " due to ", e);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
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

		// Add fields from MetaParticles
		for (Class<? extends MetaParticle> c : registeredMetaParticles) {
			fields = ParticleMapper.mergeFields(fields,
					ParticleMapper.getFields(c));
		}
		nrOfOutputFields = fields.size();
		declarer.declare(fields);
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
				particle.setTimestamp(now);
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
	 *            Current time
	 */
	private void emitTimerTicks(long now) {
		// Do we have to emit timerTicks?
		if (mainTimerTickFreqMs != 0) {
			// firstTime? start from now
			if (lastKnownNow == -1) {
				lastKnownNow = now;
				// emit first timerTick at the same time as the particle
				emitParticle(new TimerTickParticle(now));
			} else {
				// emit zero or more timerTicks up to now
				while ((now - lastKnownNow) >= mainTimerTickFreqMs) {
					lastKnownNow = lastKnownNow + mainTimerTickFreqMs;
					emitParticle(new TimerTickParticle(lastKnownNow));
				}
			}
		}
	}

	/**
	 * Emit a {@link Particle}, both {@link DataParticle} and
	 * {@link MetaParticle} are possible.
	 * 
	 * @param particle
	 *            {@link Particle} to emit
	 */
	public void emitParticle(Particle particle) {
		if (particle != null) {
			if (particle instanceof MetaParticle) {
				((MetaParticle) particle).setOriginId(originId);
			}
			collector
					.emit(ParticleMapper.particleToValues(particle,
							nrOfOutputFields), UUID.randomUUID().toString());
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
