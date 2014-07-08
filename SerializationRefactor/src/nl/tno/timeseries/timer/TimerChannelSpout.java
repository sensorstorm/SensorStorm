package nl.tno.timeseries.timer;

import java.util.HashMap;
import java.util.Map;

import nl.tno.timeseries.interfaces.DataParticle;
import nl.tno.timeseries.interfaces.Fetcher;
import nl.tno.timeseries.interfaces.Particle;
import nl.tno.timeseries.mapper.ParticleMapper;
import nl.tno.timeseries.stormcomponents.ChannelSpout;
import backtype.storm.tuple.Fields;

public class TimerChannelSpout extends ChannelSpout {
	private static final long serialVersionUID = 2763848654616526657L;
	private Long mainTimerTickFreq;
	private Boolean useParticleTime;
	private Map<String, Long> lastKnownNows = new HashMap<String, Long>();

	public TimerChannelSpout(Fetcher fetcher, Class<? extends Particle> outputParticleClass) {
		super(fetcher, outputParticleClass);

		mainTimerTickFreq = 1L;
		useParticleTime = true;
	}

	public TimerChannelSpout(Fetcher fetcher, Class<? extends Particle> outputParticleClass, Boolean useParticleTime,
			Long mainTimerTickFreq) {
		super(fetcher, outputParticleClass);

		this.mainTimerTickFreq = mainTimerTickFreq;
		this.useParticleTime = useParticleTime;
	}

	@Override
	public void nextTuple() {
		// get next particle
		DataParticle particle = fetcher.fetchParticle();

		// emit particle together with optional leading timerTicks
		if (particle != null) {
			Long now;
			if (useParticleTime) {
				now = particle.getSequenceNr();
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
			collector.emit(ParticleMapper.particleToValues(particle, nrOfOutputFields));
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

			TimerTickParticle timerTickParticle = new TimerTickParticle(channelId, lastKnownNow);
			collector.emit(ParticleMapper.particleToValues(timerTickParticle, nrOfOutputFields));
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
		fields = ParticleMapper.mergeFields(fields, ParticleMapper.getFields(TimerTickParticle.class));
		return fields;
	}

}
