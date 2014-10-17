package nl.tno.timeseries.testapp;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import nl.tno.storm.configuration.api.StormConfigurationException;
import nl.tno.storm.configuration.api.ExternalStormConfiguration;
import nl.tno.timeseries.annotation.FetcherDeclaration;
import nl.tno.timeseries.config.FetcherConfigManager;
import nl.tno.timeseries.interfaces.DataParticle;
import nl.tno.timeseries.interfaces.Fetcher;
import backtype.storm.task.TopologyContext;

@FetcherDeclaration(outputs = { MyDataParticle.class })
public class MyConfigFetcher implements Fetcher {
	private static final long serialVersionUID = -4783593429530609215L;
	long time = 0;

	enum DataParticleType {
		l, d, s
	};

	DataParticleType dataParticleType = DataParticleType.l;

	private final AtomicReference<Double> myvardouble = new AtomicReference<Double>(
			0.0);
	private final AtomicReference<Long> myvarlong = new AtomicReference<Long>(
			0L);
	private final AtomicReference<String> myvarstring = new AtomicReference<String>(
			"");

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormNativeConfig,
			ExternalStormConfiguration stormConfiguration,
			TopologyContext context) throws Exception {
		FetcherConfigManager fetcherConfigManager = new FetcherConfigManager(
				stormConfiguration, "myconfigfetcher");
		try {
			fetcherConfigManager.registerParameter("myvarlong", myvarlong);
			fetcherConfigManager.registerParameter("myvardouble", myvardouble);
			fetcherConfigManager.registerParameter("myvarstring", myvarstring);
			fetcherConfigManager.loadParameters();
		} catch (StormConfigurationException e) {
			System.out.println("Can not load parameters.");
		}

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
		switch (dataParticleType) {
		case l:
			dataParticleType = DataParticleType.d;
			return new MyDataParticle<Long>("Channel_1", time, myvarlong.get());
		case d:
			dataParticleType = DataParticleType.s;
			return new MyDataParticle<Double>("Channel_1", time,
					myvardouble.get());
		case s:
			dataParticleType = DataParticleType.l;
			return new MyDataParticle<String>("Channel_1", time,
					myvarstring.get());
		}

		return null;
	}

}
