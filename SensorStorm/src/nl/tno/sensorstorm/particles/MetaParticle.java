package nl.tno.sensorstorm.particles;

/**
 * A marker that this particle is a meta particle (instead of a data particle)
 * 
 * @author waaijbdvd
 * 
 */
public interface MetaParticle extends Particle {

	public String getOriginId();

	public void setOriginId(String originId);
}
