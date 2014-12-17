package nl.tno.timeseries.testapp;

import java.util.List;
import java.util.Map;

import nl.tno.sensorstorm.annotation.OperationDeclaration;
import nl.tno.sensorstorm.operations.BatchOperation;
import nl.tno.sensorstorm.particles.DataParticle;
import nl.tno.sensorstorm.particles.DataParticleBatch;
import nl.tno.storm.configuration.api.ExternalStormConfiguration;

@OperationDeclaration(inputs = { MyDataParticle.class }, outputs = {})
public class MyBatchOperation implements BatchOperation {
	private static final long serialVersionUID = 773649574489299505L;
	private String fieldGrouper;

	@Override
	public void init(String fieldGrouper, long startTimestamp,
			@SuppressWarnings("rawtypes") Map stormNativeConfig,
			ExternalStormConfiguration stormConfiguration) {
		this.fieldGrouper = fieldGrouper;
		System.out.println("init myBatchOperation at " + startTimestamp);
	}

	@Override
	public List<DataParticle> execute(DataParticleBatch inputParticles) {
		if (inputParticles != null) {
			System.out.print("Bacth Operation fieldGrouper " + fieldGrouper
					+ " batch received :[");
			for (DataParticle inputParticle : inputParticles) {
				System.out.print(inputParticle + ", ");
			}
			System.out.println("]");
		}
		return null;
	}

}
