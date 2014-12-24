package nl.tno.timeseries.testapp;

import java.util.Map;

import nl.tno.sensorstorm.annotation.FetcherDeclaration;
import nl.tno.sensorstorm.fetchers.Fetcher;
import nl.tno.sensorstorm.particles.DataParticle;
import nl.tno.storm.configuration.api.ExternalStormConfiguration;
import backtype.storm.task.TopologyContext;

@FetcherDeclaration(outputs = { MyDataParticle.class })
public class MyFetcher implements Fetcher {
	private static final long serialVersionUID = -4783593429530609215L;
	long time = 0;
	int myId = 0;
	int nrOfIds;

	public MyFetcher(int nrOfIds) {
		this.nrOfIds = nrOfIds;
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormNativeConfig,
			ExternalStormConfiguration stormConfiguration,
			TopologyContext context) {
		System.out.println("MyFetcher prepare");
	}

	@Override
	public void activate() {
	}

	@Override
	public void deactivate() {
	}

	@Override
	public DataParticle fetchParticle() {
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
		}
		if (myId == 0) {
			time = time + 1000;
		}

		MyDataParticle<Double> myDataParticle = new MyDataParticle<Double>(
				"ID_" + myId, time, 1.0);
		myId = (myId + 1) % nrOfIds;
		return myDataParticle;
	}

}
