package nl.tno.timeseries.testapp;

import java.util.List;
import java.util.Map;

import nl.tno.storm.configuration.api.ExternalStormConfiguration;
import nl.tno.timeseries.annotation.OperationDeclaration;
import nl.tno.timeseries.gracefullshutdown.GracefulShutdownParticleHandler;
import nl.tno.timeseries.gracefullshutdown.GracefullShutdownInterface;
import nl.tno.timeseries.interfaces.DataParticle;
import nl.tno.timeseries.interfaces.OperationException;
import nl.tno.timeseries.interfaces.SingleOperation;
import nl.tno.timeseries.timer.TimerControllerInterface;

@OperationDeclaration(inputs = { MyDataParticle.class }, outputs = {}, metaParticleHandlers = { GracefulShutdownParticleHandler.class })
public class MyGracefullShutdownOperation implements SingleOperation,
		GracefullShutdownInterface {
	private static final long serialVersionUID = 773649574489299505L;
	TimerControllerInterface timerController = null;
	private String channelId;

	@Override
	public void init(String channelId,
			@SuppressWarnings("rawtypes") Map stormNativeConfig,
			ExternalStormConfiguration stormConfiguration) {
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

	@Override
	public void gracefullShutdown() {
		System.out.println("Channel " + channelId + " Gracefull shutdown!");
	}

}
