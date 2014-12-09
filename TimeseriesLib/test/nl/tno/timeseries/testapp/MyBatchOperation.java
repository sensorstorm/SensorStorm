package nl.tno.timeseries.testapp;

import java.util.List;
import java.util.Map;

import nl.tno.storm.configuration.api.ExternalStormConfiguration;
import nl.tno.timeseries.annotation.OperationDeclaration;
import nl.tno.timeseries.operations.BatchOperation;
import nl.tno.timeseries.operations.OperationException;
import nl.tno.timeseries.particles.DataParticle;
import nl.tno.timeseries.particles.DataParticleBatch;

@OperationDeclaration(inputs = { MyDataParticle.class }, outputs = {})
public class MyBatchOperation implements BatchOperation {
	private static final long serialVersionUID = 773649574489299505L;
	private String fieldGrouper;

	@Override
	public void init(String fieldGrouper,
			@SuppressWarnings("rawtypes") Map stormNativeConfig,
			ExternalStormConfiguration stormConfiguration) {
		this.fieldGrouper = fieldGrouper;
	}

	@Override
	public void prepareForFirstParticle(long startTimestamp)
			throws OperationException {
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
