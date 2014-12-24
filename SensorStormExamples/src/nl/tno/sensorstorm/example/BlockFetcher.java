package nl.tno.sensorstorm.example;

import java.util.Map;

import nl.tno.sensorstorm.api.annotation.FetcherDeclaration;
import nl.tno.sensorstorm.api.particles.DataParticle;
import nl.tno.sensorstorm.api.processing.Fetcher;
import nl.tno.storm.configuration.api.ExternalStormConfiguration;
import backtype.storm.task.TopologyContext;

@FetcherDeclaration(outputs = SensorParticle.class)
public class BlockFetcher implements Fetcher {

	private static final long serialVersionUID = -6406591596237697067L;
	private long lastParticleTimestamp = 0;

	@Override
	public void activate() {
		lastParticleTimestamp = System.currentTimeMillis();
	}

	@Override
	public void deactivate() {
	}

	@Override
	public DataParticle fetchParticle() {
		long now = System.currentTimeMillis();
		if (now > (lastParticleTimestamp + 100)) {
			// Time for the next particle
			lastParticleTimestamp += 100;
			double value = ((lastParticleTimestamp / 1000) % 2) == 0 ? -1 : 1;
			SensorParticle sensorParticle = new SensorParticle(
					lastParticleTimestamp, "sensor_1", value);
			return sensorParticle;
		} else {
			// No particle at this moment
			return null;
		}
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConfig,
			ExternalStormConfiguration externalConfig, TopologyContext context) {
	}

}
