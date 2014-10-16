package nl.tno.timeseries.channels;

import java.util.List;
import java.util.Map;

import nl.tno.storm.configuration.api.StormConfigurationException;
import nl.tno.storm.configuration.api.ZookeeperStormConfigurationAPI;
import nl.tno.storm.configuration.impl.ZookeeperStormConfigurationFactory;
import nl.tno.timeseries.config.ConfigKeys;
import nl.tno.timeseries.config.EmptyStormConfiguration;
import nl.tno.timeseries.interfaces.FaultTolerant;
import nl.tno.timeseries.interfaces.Particle;
import nl.tno.timeseries.particles.EmitParticleInterface;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public abstract class AbstractOperationChannelBolt extends BaseRichBolt
		implements EmitParticleInterface, FaultTolerant {
	private static final long serialVersionUID = -573805125863163911L;

	protected Logger logger = LoggerFactory.getLogger(this.getClass());

	protected OutputCollector collector;
	protected String boltName;
	protected boolean ackFailAndAnchor = false;
	protected ZookeeperStormConfigurationAPI zookeeperStormConfiguration;
	protected @SuppressWarnings("rawtypes")
	Map stormNativeConfig;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormNativeConfig,
			TopologyContext context, OutputCollector collector) {
		this.stormNativeConfig = stormNativeConfig;
		this.collector = collector;
		this.boltName = context.getThisComponentId();

		// connect to the zoopkeeper configuration
		try {
			zookeeperStormConfiguration = ZookeeperStormConfigurationFactory
					.getInstance().getStormConfiguration(stormNativeConfig);
		} catch (StormConfigurationException e) {
			logger.error("Can not connect to zookeeper for get Storm configuration. Reason: "
					+ e.getMessage());
			// create empty config to avoid errors
			zookeeperStormConfiguration = new EmptyStormConfiguration();
		}

		// get ack fail and anchor status
		ackFailAndAnchor = (stormNativeConfig
				.containsKey(ConfigKeys.TOPOLOGY_FAULT_TOLERANT) && (boolean) stormNativeConfig
				.get(ConfigKeys.TOPOLOGY_FAULT_TOLERANT))
				|| (stormNativeConfig.get(Config.TOPOLOGY_MAX_SPOUT_PENDING) != null && (long) stormNativeConfig
						.get(Config.TOPOLOGY_MAX_SPOUT_PENDING) > 0);
		if (ackFailAndAnchor) {
			logger.info("Acking, Failing and Anchoring enabled");
		} else {
			logger.info("Acking, Failing and Anchoring disabled");
		}
	}

	/**
	 * Emit a list of particles, if the list is not null or empty. Based on the
	 * ackFailAndAnchor variable, anchor each particle or not
	 * 
	 * @param anchor
	 * @param particles
	 */
	public void emitParticles(Tuple anchor, List<? extends Particle> particles) {
		if (particles != null) {
			for (Particle particle : particles) {
				if (ackFailAndAnchor)
					this.emitParticle(anchor, particle);
				else
					this.emitParticle(particle);
			}
		}
	}

	@Override
	public void ack(Tuple tuple) {
		if (ackFailAndAnchor)
			collector.ack(tuple);
	}

	@Override
	public void fail(Tuple tuple) {
		if (ackFailAndAnchor)
			collector.fail(tuple);
	}

}
