package nl.tno.stormcv.fetcher;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.TopologyContext;
import nl.tno.storm.configuration.api.ZookeeperStormConfigurationAPI;
import nl.tno.stormcv.particle.Frame;
import nl.tno.stormcv.util.StreamReader;
import nl.tno.stormcv.util.adaptor.AdaptorHolder;
import nl.tno.stormcv.util.adaptor.FileAdaptor;
import nl.tno.timeseries.annotation.FetcherDeclaration;
import nl.tno.timeseries.interfaces.DataParticle;
import nl.tno.timeseries.interfaces.Fetcher;

/**
 * A Fetcher implementation that reads video files and extracts frames. Which frames should be extracted and emitted
 * by the spout is defined by the skip and groupSize parameters. Skip defines the number of frames to skip before groupSize
 * number of frames is extracted from the stream. A skip of 10 and groupSize of 3 will result in the following frames to
 * be extracted and emitted: 0,1,2,10,11,12,20,21,22 etc.
 * 
 * These files can reside on local storage or on Amazon S3 and must be provided as a List with Strings 
 * upon construction time. The elements in the list must have a specific format: protocol://path/to/file(s).
 * Each protocol must have its own @link {@link FileAdaptor} implementation. The following are supported out of the box:
 * <ul>
 * <li>AWS S3: s3://&lt;bucket&lt;/&lt;key&gt; </li>
 * <li>local files: file://&lt;absolute path&gt;</li>
 * <li>File transfer protocol: ftp://&lt;path to files&gt;</li>
 * </ul> 
 * 
 * If a location points to a directory instead of a file the directory will be expanded and all files with video extensions 
 * will be listed instead (recursively!). The expanded list will be divided among all FileFrameFetchers operating in the
 * topology. Each FileFrameFetcher will process its own list sequentially until it is done. The files will be downloaded
 * to the local filesystem in a separate thread up to a maximum of 10 unprocessed files. Note that this behavior may use 
 * a lot of bandwidth at start due to multiple {@link VideoFileFetcher}s reading at maximum speed.   
 * 
 * By default all files will get a unique streamId. If idPerFile is set to false all files will get the same streamId and 
 * frame numbering is continued as if the files form a single stream. This can be the case when reading a directory with
 * HLS segments. 
 * 
 * @author Corne Versloot
 *
 */
@FetcherDeclaration(outputs = { Frame.class })
public class VideoFileFetcher implements Fetcher {

	private static final long serialVersionUID = 8681398915636831984L;
	private Logger logger = LoggerFactory.getLogger(getClass());
	private List<String> locations;
	private int frameSkip;
	private int groupSize;
	private int sleepTime;
	private boolean useSingleId;
	private StreamReader streamReader;
	private LinkedBlockingQueue<Frame> frameQueue = new LinkedBlockingQueue<Frame>(100);
	private AdaptorHolder adaptorHolder;

	/**
	 * Constructs a VideoFileFetcher that will read streams from the provided locations.
	 * The set of locations will be equally spread over all instances of this Spout within the
	 * topology. If locations.size() > spout parallelism some spouts will read multiple streams in
	 * parallel. 
	 * @param locations a list containing urls to read
	 */
	public VideoFileFetcher (List<String> locations){
		this.locations = locations;
	}
	
	/**
	 * Sets the number of frames to skip after a frame has been read. A frameSkip of 25
	 * for a 25 fps stream will result in the extraction of 1 frame every second.
	 * @param skip the number of frames to skip, default = 25
	 * @return itself
	 */
	public VideoFileFetcher frameSkip(int skip){
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
	public VideoFileFetcher groupSize(int size){
		this.groupSize  = size;
		return this;
	}
	
	/**
	 * Sets a fixed sleep time after a frame has been extracted. This can be used to throttle the spout
	 * (can be useful when the stream is not live and can be consumed faster than real-time)
	 * @param ms milliseconds to sleep, default = 0;
	 * @return itself
	 */
	public VideoFileFetcher sleep(int ms){
		this.sleepTime = ms;
		return this;
	}
	
	/**
	 * Specify if all files read by this fetcher must get the same streamId (default is false). If set to true all files read will get
	 * the same streamId and frame numbering is continued (as if the files form a continuous stream)
	 * @param value
	 * @return
	 */
	public VideoFileFetcher singleId (boolean value){
		this.useSingleId  = value;
		return this;
	}
	
	/**
	 * Prepares the VideoFileFetcher by identifying all video files within the provided locations and
	 * dividing them among all VideoFileFitcher's present within the topology.
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map nativeConf, ZookeeperStormConfigurationAPI zookeperConf,
			TopologyContext context) throws Exception {
		this.adaptorHolder = new AdaptorHolder(nativeConf);

		List<String> original = new ArrayList<String>();
		original.addAll(locations);
		locations.clear();
		for(String dir : original){
			locations.addAll(expand(dir));
		}
		
		int nrTasks = context.getComponentTasks(context.getThisComponentId()).size();
		int taskIndex = context.getThisTaskIndex();
		
		// change the list based on the number of tasks working on it
		if (this.locations != null && this.locations.size() > 0) {
			int batchSize = (int) Math.floor(locations.size() / nrTasks) + 1;
			int start = batchSize * taskIndex;
			locations = locations.subList(start, Math.min(start + batchSize, locations.size()));
		}
	}
	

	@Override
	public void activate() {
		if(streamReader != null){
			this.deactivate();
		}
		
		LinkedBlockingQueue<String> videoList = new LinkedBlockingQueue<String>(10);
		DownloadThread dt = new DownloadThread(locations, videoList, adaptorHolder);
		new Thread(dt).start();
		
		streamReader = new StreamReader(videoList, frameSkip, groupSize, sleepTime, useSingleId,  frameQueue);
		new Thread(streamReader).start();
	}

	@Override
	public void deactivate() {
		if(streamReader != null) streamReader.stop();
		streamReader = null;
	}

	@Override
	public DataParticle fetchParticle() {
		if(streamReader == null || !streamReader.isRunning()){
			if(streamReader != null){
				streamReader.stop();
				streamReader = null;
			}
			this.activate();
		}
		
		return frameQueue.poll();
	}
	
	/**
	 * Lists all files in the specified location. If the location itself is a file the location will be the only
	 * object in the result. If the location is a directory (or AWS S3 prefix) the result will contain all files
	 * in the directory. Only files with correct extensions will be listed!
	 * @param location
	 * @return
	 */
	private List<String> expand(String location){
		FileAdaptor fl = adaptorHolder.getAdaptor(location);
		if(fl != null){
			fl.setExtensions(new String[]{".m2v", ".mp4", ".mkv", ".ts", ".flv", ".avi", ".mov", ".wmv", ".mpg", ".mpeg"});
			try {
				fl.moveTo(location);
			} catch (IOException e) {
				logger.warn("Unable to move to "+location+" due to: "+e.getMessage());
				return new ArrayList<String>();
			}
			return fl.list();
		}else return new ArrayList<String>();
	}

	/**
	 * Internal class used for donwnloading files. Each downloaded file is put in a blocking queue which is processed
	 * by the {@link VideoFileFetcher}
	 * @author Corne Versloot
	 *
	 */
	private class DownloadThread implements Runnable{

		private List<String> locations;
		private AdaptorHolder adaptorHolder;
		private LinkedBlockingQueue<String> videoList;
		
		public DownloadThread(List<String> locations, LinkedBlockingQueue<String> videoList, AdaptorHolder adaptorHolder){
			this.locations = locations;
			this.videoList = videoList;
			this.adaptorHolder = adaptorHolder;
		}
		
		@Override
		public void run() {
			while(locations.size() > 0) try{
				String location = locations.remove(0);
				FileAdaptor adaptor = adaptorHolder.getAdaptor(location);
				if(adaptor != null){
					adaptor.moveTo(location);
					File localFile = adaptor.getAsFile();
					localFile.deleteOnExit();
					videoList.put(localFile.getAbsolutePath());
				}
			}catch(Exception e){
				logger.warn("Unable to download file due to: "+e.getMessage(), e);
			}
		}
		
	}

}
