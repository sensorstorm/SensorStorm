package nl.tno.timeseries.sensor.operations.sensorgrouper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import nl.tno.sensorstorm.annotation.ChannelGrouperDeclaration;
import nl.tno.sensorstorm.fetchers.ChannelGrouper;
import nl.tno.timeseries.testapp.MyDataParticle;

@ChannelGrouperDeclaration(outputs = { MyDataParticle.class })
public class MyChannelGrouper implements ChannelGrouper {
	private static final long serialVersionUID = 2698236070436825601L;
	protected Map<String, Set<String>> channelGroups;

	public MyChannelGrouper() {
		channelGroups = new HashMap<String, Set<String>>();
		Set<String> group1 = new HashSet<String>();
		group1.add("Channel_1");
		group1.add("Channel_2");
		Set<String> group2 = new HashSet<String>();
		group2.add("Channel_2");
		group2.add("Channel_3");

		channelGroups.put("G1", group1);
		channelGroups.put("G2", group2);

		System.out.println("ChannelGrouper created.");
	}

	@Override
	public List<String> getChannelGroupIds(String channelId) {
		ArrayList<String> result = new ArrayList<String>();
		for (Entry<String, Set<String>> channelGroup : channelGroups.entrySet()) {
			if (channelGroup.getValue().contains(channelId)) {
				result.add(channelGroup.getKey());
			}
		}
		return result;
	}

	@Override
	public List<String> getAllChannelGroupIds() {
		ArrayList<String> result = new ArrayList<String>();
		result.addAll(channelGroups.keySet());

		return result;
	}

}
