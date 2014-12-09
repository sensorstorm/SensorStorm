package nl.tno.timeseries.particles.timer;

import nl.tno.timeseries.fetchers.Fetcher;
import nl.tno.timeseries.fetchers.SensorStormSpout;
import nl.tno.timeseries.mapper.ParticleMapper;
import nl.tno.timeseries.particles.DataParticle;
import nl.tno.timeseries.particles.MetaParticleUtil;
import backtype.storm.Config;
import backtype.storm.tuple.Fields;

/**
 * This spout adds main timer functionality to the SensorStormSpout. The main
 * timer can be synced to the live timer of the server the spout is running on
 * or to the time that is used in the DataParticles being produced by the
 * fetcher. The main timer is sued to insert TimerTickParticles in each Channel
 * this Spout is producing.
 * 
 * In order to be able to sync on the DataParticle timestamp , timertick
 * particles are inserted only just before the DataParticle is inserted. they
 * come in batches.
 * 
 * If the main timer is synced to the live timer and it is time in relation to
 * the mainTimerTickFreq, timerticks are emitted on all already discovered
 * channels.
 * 
 * It is up to the fetchers to create a synced time over all sources. If there
 * are two or more sources, each with their own timestamp, it is up to the
 * fetchers to determine a combined timestamp over all sources. The
 * TimerSensorStormSpout deals with only one timestamp. Also over
 * SensorStormSpouts of different type, it is up to the fetchers to create a
 * combined timestamp.
 * 
 * @author waaijbdvd
 * 
 */
public class TimerSensorStormSpout extends SensorStormSpout {
	private static final long serialVersionUID = 2763848654616526657L;
	private Long mainTimerTickFreq;
	private Boolean useParticleTime;
	private Long lastKnownNow;

	/**
	 * Create a new TimerSensorStormSpout. Default is that the main timer will
	 * be synced to the incoming particles and has a frequency of once every 60
	 * seconds.
	 * 
	 * @param config
	 *            Reference to the Storm config.
	 * @param fetcher
	 *            Reference to the fetcher instance to be used.
	 */
	public TimerSensorStormSpout(Config config, Fetcher fetcher) {
		super(config, fetcher);

		timerSensorStormSpout(config, true, 60000L);
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
	public TimerSensorStormSpout(Config config, Fetcher fetcher,
			Boolean useParticleTime, Long mainTimerTickFreq) {
		super(config, fetcher);

		timerSensorStormSpout(config, useParticleTime, mainTimerTickFreq);
	}

	/**
	 * Common constructor code.
	 * 
	 * @param config
	 * @param useParticleTime
	 * @param mainTimerTickFreq
	 */
	private void timerSensorStormSpout(Config config, Boolean useParticleTime,
			Long mainTimerTickFreq) {
		this.mainTimerTickFreq = mainTimerTickFreq;
		this.useParticleTime = useParticleTime;
		lastKnownNow = null;

		MetaParticleUtil.registerMetaParticleFieldsWithMetaParticleClass(
				config, TimerTickParticle.class);
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
	 * Emit timerTicks from lastKnowNow up to now.
	 * 
	 * @param now
	 */
	private void emitTimerTicks(Long now) {
		// Do we have to emit timerTicks?
		if (mainTimerTickFreq != 0) {
			emitTimerTicksInChannel(now);
		}
	}

	/**
	 * Emit timerTicks up to and including now
	 * 
	 * @param now
	 */
	private void emitTimerTicksInChannel(long now) {
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

	@Override
	protected Fields getOutputFields() {
		Fields fields = super.getOutputFields();
		fields = ParticleMapper.mergeFields(fields,
				ParticleMapper.getFields(TimerTickParticle.class));
		return fields;
	}

}
