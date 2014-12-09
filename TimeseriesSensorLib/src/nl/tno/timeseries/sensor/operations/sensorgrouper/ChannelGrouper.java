package nl.tno.timeseries.sensor.operations.sensorgrouper;

import java.io.Serializable;
import java.util.List;

/**
 * This interface must a ChannelGrouper object implement to be accepted as a
 * ChannelGrouper. In order to produce outputs the object must also add the
 * annotation ChannelGrouperDecleration and specify in the output parameter the
 * list of DataParticles this ChannelGrouper produces. This is being enforced by
 * the ChannelGrouperBolt.
 * 
 * @author waaijbdvd
 * 
 */
public interface _ChannelGrouper extends Serializable {

	// Reference field that is added in the storm tuple to indicate that the
	// particle has been grouped.
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
