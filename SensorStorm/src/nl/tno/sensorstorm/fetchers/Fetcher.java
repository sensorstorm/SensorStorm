package nl.tno.sensorstorm.fetchers;

import java.io.Serializable;
import java.util.Map;

import nl.tno.sensorstorm.annotation.FetcherDeclaration;
import nl.tno.sensorstorm.particles.DataParticle;
import nl.tno.sensorstorm.particles.MetaParticle;
import nl.tno.sensorstorm.stormcomponents.SensorStormSpout;
import nl.tno.storm.configuration.api.ExternalStormConfiguration;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

import com.sun.xml.internal.bind.v2.schemagen.xmlschema.Particle;

/**
 * A Fetcher retrieves data from a specific source. It is called from the
 * {@link SensorStormSpout}. The spout also takes care of {@link MetaParticle}
 * and mapping between {@link Tuple}s and {@link Particle}s.
 */
public interface Fetcher extends Serializable {

	/**
	 * Prepare method for this fetcher. Initialize streams, open connections,
	 * files, etc. It is called from the spout.open method.
	 * 
	 * @param stormNativeConfig
	 *            Native Storm configuration map
	 * @param externalStormConfiguration
	 *            Reference to the {@link ExternalStormConfiguration}
	 * @param context
	 *            Reference to the {@link TopologyContext}
	 */
	void prepare(@SuppressWarnings("rawtypes") Map stormNativeConfig,
			ExternalStormConfiguration externalStormConfiguration,
			TopologyContext context);

	/**
	 * Activate the fetcher. It is called from the spout.activate.
	 */
	void activate();

	/**
	 * Deactivate the fetcher. It is called from the spout.deactivate.
	 */
	void deactivate();

	/**
	 * Main method to return the next particle to be emitted by the spout. The
	 * fetcher should declare its {@link DataParticle}s types using the
	 * {@link FetcherDeclaration} annotation.
	 * 
	 * @return Returns the next particle, or null indicating no particle has to
	 *         be emitted.
	 */
	DataParticle fetchParticle();

}
