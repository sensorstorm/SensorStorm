package nl.tno.timeseries.timer;

import java.util.HashMap;
import java.util.Map;

import nl.tno.timeseries.channels.ChannelSpout;
import nl.tno.timeseries.interfaces.DataParticle;
import nl.tno.timeseries.interfaces.Fetcher;
import nl.tno.timeseries.mapper.ParticleMapper;
import nl.tno.timeseries.particles.MetaParticleUtil;
import backtype.storm.Config;
import backtype.storm.tuple.Fields;

/**
 * This spout adds main timer functionality to the ChannelSpout. The main timer
 * can be synced to the live timer of the server the spout is running on or to
 * the time that is used in the DataParticles being produced by the fetcher. The
 * main timer is sued to insert TimerTickParticles in each Channel this Spout is
 * producing.
 * 
 * In order to be able to sync on the DataParticle timestamp , timertick
 * particles are inserted only just before the DataParticle is inserted. they
 * come in batches.
 * 
 * If the main timer is synced to the live timer and it is time in relation to
 * the mainTimerTickFreq, timerticks are emitted on all already discovered
 * channels. Channels are discovered when the fetcher returns a DataParticle
 * with a new channelId.
 * 
 * @author waaijbdvd
 * 
 */
public class TimerChannelSpout extends ChannelSpout {
	private static final long serialVersionUID = 2763848654616526657L;
	private final Long mainTimerTickFreq;
	private final Boolean useParticleTime;
	private final Map<String, Long> lastKnownNows = new HashMap<String, Long>();

	/**
	 * Create a new TimerChannelSpout. Default is that the main timer will be
	 * synced to the incoming particles and has a frequency of once every 60
	 * seconds.
	 * 
	 * @param config
	 *            Reference to the Storm config.
	 * @param fetcher
	 *            Reference to the fetcher instance to be used.
	 */
	public TimerChannelSpout(Config config, Fetcher fetcher) {
		super(config, fetcher);

		mainTimerTickFreq = 60000L; // 1 minute
		useParticleTime = true;

		MetaParticleUtil.registerMetaParticleFieldsWithMetaParticleClass(config,
				TimerTickParticle.class);
	}

	/**
	 * Create a new TimerChannelSpout. How to sync the main timer and what its
	 * frequency is can be specified.
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
	public TimerChannelSpout(Config config, Fetcher fetcher,
			Boolean useParticleTime, Long mainTimerTickFreq) {
		super(config, fetcher);

		this.mainTimerTickFreq = mainTimerTickFreq;
		this.useParticleTime = useParticleTime;

		MetaParticleUtil.registerMetaParticleFieldsWithMetaParticleClass(config,
				TimerTickParticle.class);
	}

	/**
	 * Fetches a new DataParticle from the fetcher. Syncs the main timer.
	 * Inserts zero or more TimerTickParticles in the channel the DataParticle
	 * from the fetcher and send the DataParticle into the Channel. If the main
	 * timer is synced to the live timer and it is time in relation to the
	 * mainTimerTickFreq, timerticks are emitted on all already discovered
	 * channels.
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

			emitTimerTicksAndParticle(now, particle);
		}

		// emit realtime timerTicks if necessary
		if (!useParticleTime) {
			emitTimerTicksInAllChannels(System.currentTimeMillis());
		}
	}

	private void emitTimerTicksAndParticle(Long now, DataParticle particle) {
		// first check to see if there are timerTicks to be emitted
		if (mainTimerTickFreq != 0) {
			emitTimerTicksInChannel(particle.getChannelId(), now);
			emitParticle(particle);
		}
	}

	private void emitTimerTicksInAllChannels(Long now) {
		for (String channelId : lastKnownNows.keySet()) {
			emitTimerTicksInChannel(channelId, now);
		}
	}

	/**
	 * Emit timerTicks up to and including now into the channel
	 * 
	 * @param channelId
	 * @param now
	 */
	private void emitTimerTicksInChannel(String channelId, long now) {
		long lastKnownNow = getLastKnowNow(channelId, now);
		while (now - lastKnownNow >= mainTimerTickFreq) {
			lastKnownNow = lastKnownNow + mainTimerTickFreq;
			lastKnownNows.put(channelId, lastKnownNow);

			TimerTickParticle timerTickParticle = new TimerTickParticle(
					channelId, lastKnownNow);
			emitParticle(timerTickParticle);
		}
	}

	private long getLastKnowNow(String channelId, long now) {
		Long lastKnowNow = lastKnownNows.get(channelId);
		if (lastKnowNow == null) {
			lastKnownNows.put(channelId, now);
			return now;
		} else {
			return lastKnowNow;
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
