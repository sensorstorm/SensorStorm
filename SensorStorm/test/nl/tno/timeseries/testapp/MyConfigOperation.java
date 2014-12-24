package nl.tno.timeseries.testapp;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import nl.tno.sensorstorm.api.annotation.OperationDeclaration;
import nl.tno.sensorstorm.api.particles.DataParticle;
import nl.tno.sensorstorm.api.processing.SingleParticleOperation;
import nl.tno.sensorstorm.config.OperationConfigManager;
import nl.tno.storm.configuration.api.ExternalStormConfiguration;
import nl.tno.storm.configuration.api.StormConfigurationException;

@OperationDeclaration(inputs = { MyDataParticle.class }, outputs = {})
public class MyConfigOperation implements SingleParticleOperation {
	private static final long serialVersionUID = 773649574489299505L;
	private String fieldGrouper;

	private final AtomicReference<Long> myvar1 = new AtomicReference<Long>(0L);
	private final AtomicReference<Double> myvar2 = new AtomicReference<Double>(
			0.0);
	private final AtomicReference<String> myvar3 = new AtomicReference<String>(
			"");

	@Override
	public void init(String fieldGrouper, long startTimestamp,
			@SuppressWarnings("rawtypes") Map stormNativeConfig,
			ExternalStormConfiguration stormConfiguration) {
		this.fieldGrouper = fieldGrouper;
		System.out.println("init myConfigOperation for fieldGrouper "
				+ fieldGrouper + " at " + startTimestamp);

		OperationConfigManager operationConfigManager = new OperationConfigManager(
				stormConfiguration, "myconfigoperation");
		try {
			operationConfigManager.registerParameter("myvar1", myvar1);
			operationConfigManager.registerParameter("myvar2", myvar2);
			operationConfigManager.registerParameter("myvar3", myvar3);
			operationConfigManager.loadParameters();
		} catch (StormConfigurationException e) {
			System.out.println("Can not load parameters.");
		}
	}

	@Override
	public List<DataParticle> execute(DataParticle inputParticle) {
		if (inputParticle != null) {
			if (inputParticle instanceof MyDataParticle<?>) {
				System.out.println("Operation fieldGrouper " + fieldGrouper
						+ " MyDataParticle received " + inputParticle
						+ "  config: myvar1=" + myvar1 + ", myvar2=" + myvar2
						+ ", myvar3=" + myvar3);
			} else {
				System.out.println("Operation fieldGrouper " + fieldGrouper
						+ " Data particle received " + inputParticle);
			}
		}
		return null;
	}

}
