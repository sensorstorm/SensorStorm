package nl.tno.timeseries.testapp;

import java.util.List;
import java.util.Map;

import nl.tno.sensorstorm.annotation.OperationDeclaration;
import nl.tno.sensorstorm.operations.SingleParticleOperation;
import nl.tno.sensorstorm.particles.DataParticle;
import nl.tno.storm.configuration.api.ExternalStormConfiguration;

@OperationDeclaration(inputs = { MyDataParticle.class }, outputs = {})
public class MyOperation implements SingleParticleOperation {
	private static final long serialVersionUID = 773649574489299505L;
	private String fieldGroupValue;

	@Override
	public void init(String fieldGroupValue, long startTimestamp,
			@SuppressWarnings("rawtypes") Map stormNativeConfig,
			ExternalStormConfiguration stormConfiguration) {
		this.fieldGroupValue = fieldGroupValue;
		System.out.println("myoperation.init for fieldGroupValue "
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
