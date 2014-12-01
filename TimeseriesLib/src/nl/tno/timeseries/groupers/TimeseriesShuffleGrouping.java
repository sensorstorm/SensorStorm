package nl.tno.timeseries.groupers;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import nl.tno.timeseries.interfaces.MetaParticle;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;

public class TimeseriesShuffleGrouping implements CustomStreamGrouping,
		Serializable {

	private static final long serialVersionUID = 9192870640924179453L;
	private int numTasks;
	private List<Integer> targetTasks;

	@Override
	public void prepare(WorkerTopologyContext context, GlobalStreamId stream,
			List<Integer> targetTasks) {
		this.targetTasks = targetTasks;
		numTasks = targetTasks.size();
	}

	@Override
	public List<Integer> chooseTasks(int taskId, List<Object> values) {
		List<Integer> boltIds = new ArrayList<Integer>();
		// Is this tuple a Particle?
		try {
			if (values.size() > 3) {
				String channelId = (String) values.get(0);
				Long timestamp = (Long) values.get(1);
				String particleClassName = (String) values.get(2);
				System.out.println("channelId = " + channelId + " timestamp="
						+ timestamp + " particle class=" + particleClassName);
				Class<?> particleClass = Class.forName(particleClassName);
				// is this tuple a metaParticle?
				if (MetaParticle.class.isAssignableFrom(particleClass)) {
					// broadcast the MetaParticle tuple
					for (Integer boltId : targetTasks) {
						boltIds.add(boltId);
					}
				} else {
					// DataParticle perform normal operation.
					boltIds.add(taskId % numTasks);
				}
			}
		} catch (Exception e) {
			// error -> no or unknown particle structure, perform normal shuffle
			boltIds.add(taskId % numTasks);
		} finally {
			return boltIds;
		}
	}

}
