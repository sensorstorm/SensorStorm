package nl.tno.sensorstorm.api.particles;

import nl.tno.sensorstorm.api.processing.Operation;

/**
 * A marker interface that indicates this particle is a {@link DataParticle}
 * (instead of a {@link MetaParticle}). {@link DataParticle}s contain the actual
 * data (e.g. measurements, events) and are directly processed by
 * {@link Operation}s.
 */
public interface DataParticle extends Particle {

}
