package nl.tno.timeseries.testapp;

import java.util.List;
import java.util.Map;

import nl.tno.storm.configuration.api.ZookeeperStormConfigurationAPI;
import nl.tno.timeseries.annotation.OperationDeclaration;
import nl.tno.timeseries.interfaces.DataParticle;
import nl.tno.timeseries.interfaces.OperationException;
import nl.tno.timeseries.interfaces.SingleOperation;

@OperationDeclaration(inputs = { MyDataParticle.class }, outputs = {})
public class MyOperation implements SingleOperation {
	private static final long serialVersionUID = 773649574489299505L;
	private String channelId;

	@Override
	public void init(String channelId,
			@SuppressWarnings("rawtypes") Map stormNativeConfig,
			ZookeeperStormConfigurationAPI stormConfiguration) {
		this.channelId = channelId;
	}

	@Override
	public void prepareForFirstParticle(long startTimestamp)
			throws OperationException {
		System.out.println("init myoperation for channel " + channelId + " at "
				+ startTimestamp);
	}

	@Override
	public List<DataParticle> execute(DataParticle inputParticle) {
		if (inputParticle != null) {
			if (inputParticle instanceof MyDataParticle<?>) {
				System.out.println("Operation channel " + channelId
						+ " MyDataParticle received " + inputParticle);
			} else {
				System.out.println("Operation channel " + channelId
						+ " Data particle received " + inputParticle);
			}
		}
		return null;
	}

}
