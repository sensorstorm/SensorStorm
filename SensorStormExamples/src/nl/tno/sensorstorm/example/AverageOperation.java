package nl.tno.sensorstorm.example;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import nl.tno.sensorstorm.api.annotation.OperationDeclaration;
import nl.tno.sensorstorm.api.particles.DataParticle;
import nl.tno.sensorstorm.api.particles.DataParticleBatch;
import nl.tno.sensorstorm.api.processing.OperationException;
import nl.tno.sensorstorm.api.processing.ParticleBatchOperation;
import nl.tno.storm.configuration.api.ExternalStormConfiguration;

@OperationDeclaration(inputs = SensorParticle.class, outputs = SensorParticle.class)
public class AverageOperation implements ParticleBatchOperation {

	private static final long serialVersionUID = 5964184758382015244L;

	@Override
	public void init(String fieldGrouperValue, long startTimeStamp,
			@SuppressWarnings("rawtypes") Map stormNativeConfig,
			ExternalStormConfiguration zookeeperStormConfiguration)
			throws OperationException {

	}

	@Override
	public List<? extends DataParticle> execute(
			DataParticleBatch inputParticleBatch) throws OperationException {
		SensorParticle last = null;
		double value;
		double sum = 0;
		int cnt = 0;
		for (DataParticle p : inputParticleBatch) {
			if (p instanceof SensorParticle) {
				last = (SensorParticle) p;
				sum += last.getMeasurement();
				cnt++;
			}
		}
		if (cnt == 0) {
			value = 0;
		} else {
			value = sum / cnt;
		}
		if (last != null) {
			SensorParticle particle = new SensorParticle(last.getTimestamp(),
					last.getSensorId(), value);
			return Collections.singletonList(particle);
		} else {
			return null;
		}
	}

}
