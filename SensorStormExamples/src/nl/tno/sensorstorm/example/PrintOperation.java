package nl.tno.sensorstorm.example;

import java.util.List;
import java.util.Map;

import nl.tno.sensorstorm.api.annotation.OperationDeclaration;
import nl.tno.sensorstorm.api.particles.DataParticle;
import nl.tno.sensorstorm.api.processing.OperationException;
import nl.tno.sensorstorm.api.processing.SingleParticleOperation;
import nl.tno.storm.configuration.api.ExternalStormConfiguration;

@OperationDeclaration(inputs = DataParticle.class)
public class PrintOperation implements SingleParticleOperation {

	private static final long serialVersionUID = -5799424083707865813L;

	@Override
	public void init(String fieldGrouperValue, long startTimeStamp,
			@SuppressWarnings("rawtypes") Map stormNativeConfig,
			ExternalStormConfiguration zookeeperStormConfiguration)
			throws OperationException {
	}

	@Override
	public List<? extends DataParticle> execute(DataParticle inputParticle)
			throws OperationException {
		System.err.println("Received Particle: " + inputParticle);
		return null;
	}

}
