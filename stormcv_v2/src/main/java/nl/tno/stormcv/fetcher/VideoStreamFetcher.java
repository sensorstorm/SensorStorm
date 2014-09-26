package nl.tno.stormcv.fetcher;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import backtype.storm.task.TopologyContext;
import nl.tno.storm.configuration.api.ZookeeperStormConfigurationAPI;
import nl.tno.stormcv.particle.Frame;
import nl.tno.stormcv.util.StreamReader;
import nl.tno.timeseries.annotation.FetcherDeclaration;
import nl.tno.timeseries.interfaces.DataParticle;
import nl.tno.timeseries.interfaces.Fetcher;

/**
 * A Fetcher implementation that reads video streams and extracts frames. Which frames should be extracted and emitted
 * by the spout is defined by the skip and groupSize parameters. Skip defines the number of frames to skip before groupSize
 * number of frames is extracted from the stream. A skip of 10 and groupSize of 3 will result in the following frames to
 * be extracted and emitted: 0,1,2,10,11,12,20,21,22 etc.
 * 
 * @author Corne Versloot
 *
 */
@FetcherDeclaration(outputs = { Frame.class })
public class VideoStreamFetcher implements Fetcher {

	private static final long serialVersionUID = -5889676372965152669L;
	private List<String> locations;
	private int groupSize = 1;
	private int sleepTime = 0;
	private int frameSkip = 25;
	protected LinkedBlockingQueue<Frame> frameQueue = new LinkedBlockingQueue<Frame>(20);
	protected Map<String, StreamReader> streamReaders;

	/**
	 * Constructs a VideoStreamFetcher that will read streams from the provided locations.
	 * The set of locations will be equally spread over all instances of this Spout within the
	 * topology. If locations.size() > spout parallelism some spouts will read multiple streams in
	 * parallel. 
	 * @param locations a list containing urls to read
	 */
	public VideoStreamFetcher (List<String> locations){
		this.locations = locations;
	}
	
	/**
	 * Sets the number of frames to skip after a frame has been read. A frameSkip of 25
	 * for a 25 fps stream will result in the extraction of 1 frame every second.
	 * @param skip the number of frames to skip, default = 25
	 * @return itself
	 */
	public VideoStreamFetcher frameSkip(int skip){
		this.frameSkip = skip;
		return this;
	}
	
	/**
	 * Sets the number of subsequent frames to be extract from a group. 
	 * If the spout is configured with a frameSkip of 5 and a groupSize of 2 it will
	 * read 2 frames out of every 5; i.e. frames {0, 1, 5, 6, 10, 11, etc}
	 * @param size the number of frames to be read. Default = 1
	 * @return itself
	 */
	public VideoStreamFetcher groupSize(int size){
		this.groupSize  = size;
		return this;
	}
	
	/**
	 * Sets a fixed sleep time after a frame has been extracted. This can be used to throttle the spout
	 * (can be useful when the stream is not live and can be consumed faster than real-time)
	 * @param ms milliseconds to sleep, default = 0;
	 * @return itself
	 */
	public VideoStreamFetcher sleep(int ms){
		this.sleepTime = ms;
		return this;
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map nativeConf, ZookeeperStormConfigurationAPI zookeperConf,
			TopologyContext context) throws Exception {
		int nrTasks = context.getComponentTasks(context.getThisComponentId()).size();
		int taskIndex = context.getThisTaskIndex();
		
		// change the list based on the number of tasks working on it
		if(this.locations != null && this.locations.size() > 0){
			int batchSize = (int) Math.floor(locations.size() / nrTasks);
			int start = batchSize * taskIndex;
			locations = locations.subList(start, Math.min(start + batchSize, locations.size()));
		}
	}
	
	/**
	 * Activates this Fetcher by starting a @link {@link StreamReader} for each URL 
	 * assigned to the Fetcher instance (depends on the number of Fetchers running in the
	 * Topology). Each StreamReader runs in its own thread which is managed by the 
	 * StreamFetcher.
	 */
	@Override
	public void activate() {
		if(streamReaders != null){
			this.deactivate();
		}
		streamReaders = new HashMap<String, StreamReader>();
		for(String location : locations){
			
			String streamId = ""+location.hashCode();
			if(location.contains("/")){
				streamId = location.substring(location.lastIndexOf("/")+1) + "_" + streamId;
			}
			StreamReader reader = new StreamReader(streamId, location, frameSkip, groupSize, sleepTime, frameQueue);
			streamReaders.put(location, reader);
			new Thread(reader).start();
		}
	}

	/**
	 * Deactivates itself by stopping all @link {@link StreamReader} threads
	 */
	@Override
	public void deactivate() {
		if(streamReaders != null) for(String location : streamReaders.keySet()){
			streamReaders.get(location).stop();
		}
		streamReaders = null;
	}

	/**
	 * @return the next @link {@link Frame} if available, else NULL
	 */
	@Override
	public DataParticle fetchParticle() {
		if(streamReaders == null) this.activate();
		return frameQueue.poll();
	}

}
