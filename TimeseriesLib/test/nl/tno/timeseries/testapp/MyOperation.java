package nl.tno.timeseries.testapp;

import java.util.List;
import java.util.Map;

import nl.tno.storm.configuration.api.ExternalStormConfiguration;
import nl.tno.timeseries.annotation.OperationDeclaration;
import nl.tno.timeseries.operations.OperationException;
import nl.tno.timeseries.operations.SingleOperation;
import nl.tno.timeseries.particles.DataParticle;

@OperationDeclaration(inputs = { MyDataParticle.class }, outputs = {})
public class MyOperation implements SingleOperation {
	private static final long serialVersionUID = 773649574489299505L;
	private String fieldGroupValue;

	@Override
	public void init(String fieldGroupValue,
			@SuppressWarnings("rawtypes") Map stormNativeConfig,
			ExternalStormConfiguration stormConfiguration) {
		this.fieldGroupValue = fieldGroupValue;
		System.out.println("myoperation.init for fieldGroupValue "
				+ fieldGroupValue);
	}

	@Override
	public void prepareForFirstParticle(long startTimestamp)
			throws OperationException {
		System.out
				.println("myoperation.prepareForFirstParticle for fieldGroupValue "
						+ fieldGroupValue + " at " + startTimestamp);
	}

	@Override
	public List<DataParticle> execute(DataParticle inputParticle) {
		if (inputParticle != null) {
			if (inputParticle instanceof MyDataParticle<?>) {
				System.out.println("myoperation.MyDataParticle received "
						+ inputParticle);
			} else {
				System.out.println("myoperation.Data particle received "
						+ inputParticle);
			}
		}
		return null;
	}

}
