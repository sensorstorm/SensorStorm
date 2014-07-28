package nl.tno.timeseries.interfaces;

import java.io.Serializable;
import java.util.List;

public interface ChannelGrouper extends Serializable {

	public final static String GROUPED_PARTICLE_FIELD = "GroupedParticle";

	public List<String> getChannelGroupId(String channelId);

}
