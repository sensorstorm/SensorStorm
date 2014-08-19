package nl.tno.timeseries.testapp;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import nl.tno.timeseries.annotation.ChannelGrouperDeclaration;
import nl.tno.timeseries.interfaces.ChannelGrouper;

@ChannelGrouperDeclaration(outputs = { Measurement.class })
public class MyChannelGrouper implements ChannelGrouper {
	private static final long serialVersionUID = 2698236070436825601L;
	protected Map<String, Set<String>> channelGroups;

	public MyChannelGrouper() {
		channelGroups = new HashMap<String, Set<String>>();
		Set<String> group1 = new HashSet<String>();
		group1.add("S1");
		group1.add("S2");
		Set<String> group2 = new HashSet<String>();
		group2.add("S2");
		group2.add("S3");

		channelGroups.put("G1", group1);
		channelGroups.put("G2", group2);

	}

	@Override
	public List<String> getChannelGroupId(String channelId) {
		ArrayList<String> result = new ArrayList<String>();
		for (Entry<String, Set<String>> channelGroup : channelGroups.entrySet()) {
			if (channelGroup.getValue().contains(channelId)) {
				result.add(channelGroup.getKey());
			}
		}
		return result;
	}

}
