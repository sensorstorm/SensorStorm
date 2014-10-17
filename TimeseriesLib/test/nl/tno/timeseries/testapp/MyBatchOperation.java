package nl.tno.timeseries.testapp;

import java.util.List;
import java.util.Map;

import nl.tno.storm.configuration.api.ExternalStormConfiguration;
import nl.tno.timeseries.annotation.OperationDeclaration;
import nl.tno.timeseries.interfaces.BatchOperation;
import nl.tno.timeseries.interfaces.DataParticle;
import nl.tno.timeseries.interfaces.DataParticleBatch;
import nl.tno.timeseries.interfaces.OperationException;

@OperationDeclaration(inputs = { MyDataParticle.class }, outputs = {})
public class MyBatchOperation implements BatchOperation {
	private static final long serialVersionUID = 773649574489299505L;
	private String channelId;

	@Override
	public void init(String channelID,
			@SuppressWarnings("rawtypes") Map stormNativeConfig,
			ExternalStormConfiguration stormConfiguration) {
		this.channelId = channelID;
	}

	@Override
	public void prepareForFirstParticle(long startTimestamp)
			throws OperationException {
		System.out.println("init myBatchOperation at " + startTimestamp);
	}

	@Override
	public List<DataParticle> execute(DataParticleBatch inputParticles) {
		if (inputParticles != null) {
			System.out.print("Bacth Operation channel " + channelId
					+ " batch received :[");
			for (DataParticle inputParticle : inputParticles) {
				System.out.print(inputParticle + ", ");
			}
			System.out.println("]");
		}
		return null;
	}

}
