package nl.tno.timeseries.testapp;

import java.util.Map;

import nl.tno.storm.configuration.api.ExternalStormConfiguration;
import nl.tno.timeseries.annotation.FetcherDeclaration;
import nl.tno.timeseries.interfaces.DataParticle;
import nl.tno.timeseries.interfaces.Fetcher;
import backtype.storm.task.TopologyContext;

@FetcherDeclaration(outputs = { MyDataParticle.class })
public class MyFetcher implements Fetcher {
	private static final long serialVersionUID = -4783593429530609215L;
	long time = 0;
	int channelId = 0;
	int nrOfChannels = 5;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormNativeConfig,
			ExternalStormConfiguration stormConfiguration,
			TopologyContext context) throws Exception {
		System.out.println("MyFetcher prepaper");
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
		if (channelId == 0) {
			time = time + 1000;
		}

		MyDataParticle<Double> myDataParticle = new MyDataParticle<Double>(
				"Channel_" + channelId, time, 1.0);
		channelId = (channelId + 1) % nrOfChannels;
		return myDataParticle;
	}

}
