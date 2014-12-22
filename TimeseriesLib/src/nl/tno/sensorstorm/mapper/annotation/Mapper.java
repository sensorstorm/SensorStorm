package nl.tno.sensorstorm.mapper.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import nl.tno.sensorstorm.mapper.api.CustomParticlePojoMapper;

import com.sun.xml.internal.bind.v2.schemagen.xmlschema.Particle;

/**
 * Defines a custom mapper for a {@link Particle} class.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE })
public @interface Mapper {
	/**
	 * The {@link CustomParticlePojoMapper} to be used.
	 */
	@SuppressWarnings("rawtypes")
	Class<? extends CustomParticlePojoMapper> value();

}
