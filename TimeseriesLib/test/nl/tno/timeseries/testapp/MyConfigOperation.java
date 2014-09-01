package nl.tno.timeseries.testapp;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import nl.tno.storm.configuration.api.StormConfiguration;
import nl.tno.timeseries.annotation.OperationDeclaration;
import nl.tno.timeseries.interfaces.DataParticle;
import nl.tno.timeseries.interfaces.SingleOperation;
import nl.tno.timeseries.timer.TimerParticleHandler;

@OperationDeclaration(inputs = { MyDataParticle.class }, outputs = {}, metaParticleHandlers = { TimerParticleHandler.class })
public class MyConfigOperation implements SingleOperation {
	private static final long serialVersionUID = 773649574489299505L;
	private String channelId;

	private Integer bufSize;
	private AtomicInteger bufSizeA;

	public int getBufSize() {
		synchronized (bufSize) {
			return bufSize;
		}

	}

	public synchronized void setBufSize(int bufSize) {
		this.bufSize = bufSize;
	}

	@Override
	public void init(String channelID, long startTimestamp,
			StormConfiguration stormConfiguration) {
		this.channelId = channelID;
		System.out.println("init myoperation for channel " + channelID + " at "
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
