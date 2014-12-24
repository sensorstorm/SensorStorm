package nl.tno.sensorstorm.example;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import nl.tno.sensorstorm.api.particles.DataParticle;
import nl.tno.sensorstorm.api.particles.DataParticleBatch;
import nl.tno.sensorstorm.api.processing.Batcher;
import nl.tno.sensorstorm.api.processing.BatcherException;
import nl.tno.storm.configuration.api.ExternalStormConfiguration;

public class WindowBatcher implements Batcher {

	private static final int WINDOW_SIZE = 10;
	private Queue<DataParticle> window;

	@SuppressWarnings("rawtypes")
	@Override
	public void init(String fieldGrouper, long startTimestamp,
			Map stormNativeConfig,
			ExternalStormConfiguration externalStormConfiguration)
			throws BatcherException {
		window = new LinkedList<DataParticle>();
	}

	@Override
	public List<DataParticleBatch> batch(DataParticle inputParticle)
			throws BatcherException {
		window.add(inputParticle);
		while (window.size() > WINDOW_SIZE) {
			window.poll();
		}
		DataParticleBatch batch = new DataParticleBatch();
		batch.addAll(window);
		return Collections.singletonList(batch);
	}

}
