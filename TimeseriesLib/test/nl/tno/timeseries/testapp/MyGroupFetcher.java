package nl.tno.timeseries.testapp;

import java.util.Map;

import nl.tno.sensorstorm.annotation.FetcherDeclaration;
import nl.tno.sensorstorm.fetchers.Fetcher;
import nl.tno.sensorstorm.particles.DataParticle;
import nl.tno.storm.configuration.api.ExternalStormConfiguration;
import backtype.storm.task.TopologyContext;

@FetcherDeclaration(outputs = { MyDataParticle.class })
public class MyGroupFetcher implements Fetcher {
	private static final long serialVersionUID = -4783593429530609215L;
	long time = 0;
	private final String[] channels = { "Channel_1", "Channel_2", "Channel_3" };
	private int channelIndex = 0;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormNativeConfig,
			ExternalStormConfiguration stormConfiguration,
			TopologyContext context) throws Exception {
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
		time = time + 1000;
		return new MyDataParticle<Double>(getChannel(), time, 1.0);
	}

	private String getChannel() {
		String channel = channels[channelIndex];
		channelIndex++;
		if (channelIndex >= channels.length) {
			channelIndex = 0;
		}
		return channel;
	}

}
