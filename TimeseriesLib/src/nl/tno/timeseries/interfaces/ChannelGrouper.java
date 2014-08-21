package nl.tno.timeseries.interfaces;

import java.io.Serializable;
import java.util.List;

public interface ChannelGrouper extends Serializable {

	// Reference field to indicate
	public static String GROUPED_PARTICLE_FIELD = "grouped-particle";

	/**
	 * Return a list of all channels this channelid (particle) must be send to.
	 * 
	 * @param channelId
	 * @return
	 */
	public List<String> getChannelGroupIds(String channelId);

	/**
	 * Used to broadcast metaParticles.
	 * 
	 * @return Return a list of all possible channelIds this channelGrouper can
	 *         produce.
	 */
	public List<String> getAllChannelGroupIds();

}
