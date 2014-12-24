package nl.tno.sensorstorm.particles.gracefullshutdown;

/**
 * Interface to be used by operations, which specifies the gracefull shutdown
 * hook
 * 
 * @author waaijbdvd
 * 
 */
public interface GracefullShutdownInterface {

	/**
	 * When this method is called, the operation must save all its (optional)
	 * state and prepare for a shutdown of the topology. No further particles,
	 * meta or data, will be send to this operation.
	 */
	public void gracefullShutdown();
}
