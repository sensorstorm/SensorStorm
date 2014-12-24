package nl.tno.sensorstorm.mapper.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import nl.tno.sensorstorm.mapper.api.CustomParticlePojoMapper;
import nl.tno.sensorstorm.particles.Particle;

/**
 * Defines a custom mapper for a {@link Particle} class.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Mapper {
	/**
	 * The {@link CustomParticlePojoMapper} to be used for mapping this
	 * {@link Particle} class.
	 */
	@SuppressWarnings("rawtypes")
	Class<? extends CustomParticlePojoMapper> value();

}
