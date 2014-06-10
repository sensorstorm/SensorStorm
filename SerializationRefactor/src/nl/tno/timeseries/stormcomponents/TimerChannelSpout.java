package nl.tno.timeseries.stormcomponents;

import java.util.HashMap;
import java.util.Map;

import nl.tno.timeseries.interfaces.DataParticle;
import nl.tno.timeseries.interfaces.Fetcher;
import nl.tno.timeseries.interfaces.Particle;
import nl.tno.timeseries.mapper.ParticleMapper;
import nl.tno.timeseries.particles.TimerTickParticle;

public class TimerChannelSpout extends ChannelSpout {
	private static final long serialVersionUID = 2763848654616526657L;
	private Long mainTimerTickFreq;
	private Boolean useParticleTime;
	private Map<String, Long> lastKnownNows = new HashMap<String, Long>();

	public TimerChannelSpout(Fetcher fetcher, Class<? extends Particle> outputParticleClass) {
		super(fetcher, outputParticleClass);
		
		mainTimerTickFreq = 10L;
		useParticleTime = true;
	}

	
	@Override
	public void nextTuple() {
		// get next particle
		DataParticle particle = fetcher.fetchParticle();

		// emit particle together with optional leading timerTicks
		Long timestamp = getTimestamp(particle);
		if (timestamp != null) {
			emitTimerTicksAndParticle(timestamp, particle);
		}
		
//		if (particle != null) {
//			Long timestamp = getTimestamp(particle);
//			emitTimerTicksAndParticle(timestamp, particle);
//		} else {	
//			// perhaps liveTime wants to emit a timerTick
//			Long timestamp = getTimestamp(null);
//			if (timestamp != null) {
//				emitTimerTicksAndParticle(timestamp, null);
//			} // no tuple and no timestamp means nothing to emit
//		}
//
	}

	
	/**
	 * return the "now"timestamp. Which is system time or the timestamp form the particle depending on useParticleTime setting
	 * @param particle
	 * @return
	 */
	private Long getTimestamp(DataParticle particle) {
		if (useParticleTime) {
			if (particle == null) {
				return null;
			} else {
				return particle.getSequenceNr();
			}
		} else { 
			return System.currentTimeMillis();
		}
	}

	
	private void emitTimerTicksAndParticle(Long now, DataParticle particle) {
		// first check to see if there are timerTicks to be emitted
		if (mainTimerTickFreq != 0) {
			if (particle != null) {
				emitTimerTicksInChannel(particle.getChannelId(), now);
				collector.emit(ParticleMapper.particleToValues(particle));
			} else {
				// no particle to emit, but still check all timers
				for (String channelId : lastKnownNows.keySet()) {
					emitTimerTicksInChannel(channelId, now);	
				}
			}
		}
	}
	
	/**
	 * Emit timerTicks up to and including now into the channel
	 * @param channelId
	 * @param now
	 */
	private void emitTimerTicksInChannel(String channelId, long now) {
		long lastKnownNow = getLastKnowNow(channelId, now);
		while (now - lastKnownNow >= mainTimerTickFreq) {
			lastKnownNow = lastKnownNow + mainTimerTickFreq;
			setLastKnowNow(channelId, lastKnownNow);
			
			TimerTickParticle timerTickParticle = new TimerTickParticle(channelId, lastKnownNow);
			collector.emit(ParticleMapper.particleToValues(timerTickParticle));
		}
	}

	
	private long getLastKnowNow(String sensorID, long now) {
		Long lastKnowNow = lastKnownNows.get(sensorID);
		if (lastKnowNow == null) {
			setLastKnowNow(sensorID, now);
			return now;
		} else {
			return lastKnowNow;
		}
	}
	
	
	private void setLastKnowNow(String sensorID, long lastKnownNow) {
		lastKnownNows.put(sensorID, lastKnownNow);
	}



}
