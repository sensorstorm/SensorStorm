package nl.tno.storm.configuration.impl;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import nl.tno.storm.configuration.api.ConfigurationListener;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;

public class WatcherHelper implements Watcher {

	private final String mapPath;
	private final List<ConfigurationListener> listeners = new CopyOnWriteArrayList<>();
	private final Set<String> watchedChildren = new HashSet<String>();
	private final ZookeeperStormConfiguration zkConfiguration;

	public WatcherHelper(ZookeeperStormConfiguration zkConfiguration, String mapPath) throws Exception {
		this.zkConfiguration = zkConfiguration;
		this.mapPath = mapPath;
		watchMap();
		watchChildren();
	}

	/**
	 * Create a watcher for the parent node; triggers
	 * EventType.NodeChildrenChanged and EventType.NodeDeleted
	 * 
	 * @throws Exception
	 */
	private void watchMap() throws Exception {
		this.zkConfiguration.zkClient.getChildren().usingWatcher(this).forPath(this.mapPath);
	}

	/**
	 * Set a data watcher on all children. By keeping track of the watchers we
	 * already set we avoid setting double watchers on the same node.
	 * 
	 * @throws Exception
	 */
	private void watchChildren() throws Exception {
		for (String childKey : this.zkConfiguration.zkClient.getChildren().forPath(mapPath)) {
			String path = mapPath + "/" + childKey;
			if (!watchedChildren.contains(path)) {
				this.zkConfiguration.zkClient.getData().usingWatcher(this).forPath(path);
				watchedChildren.add(path);
			}
		}
	}

	public void addListener(ConfigurationListener listener) {
		this.listeners.add(listener);
	}

	public void removeListener(ConfigurationListener listener) {
		this.listeners.remove(listener);
	}

	@Override
	public void process(WatchedEvent e) {
		try {
			if (e.getType() == EventType.NodeChildrenChanged) {
				// Reset the children watcher
				this.zkConfiguration.zkClient.getChildren().usingWatcher(this).forPath(e.getPath());
			} else if (e.getType() == EventType.NodeDataChanged) {
				// Data of a node changed. We reset the watcher in
				// watchChildren()
				watchedChildren.remove(e.getPath());
			} else if (e.getType() == EventType.NodeDeleted) {
				// When a node is deleted the NodeChildrenChanged event also
				// fires. By ignoring this event we avoid have double events.
				// When the node comes back we have to create a new watcher, so
				// we remove it form watchedChildren.
				watchedChildren.remove(e.getPath());
				return;
			}
			watchChildren();

			// Notify all listeners
			for (ConfigurationListener listener : listeners) {
				listener.configurationChanged(zkConfiguration.pathToMap(mapPath));
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
}
