package nl.tno.sensorstorm.syncbuffertest;

import java.util.List;

import junit.framework.TestCase;
import nl.tno.sensorstorm.api.particles.AbstractMetaParticle;
import nl.tno.sensorstorm.api.particles.DataParticle;
import nl.tno.sensorstorm.api.particles.MetaParticle;
import nl.tno.sensorstorm.api.particles.Particle;
import nl.tno.sensorstorm.gracefullshutdown.GracefullShutdownParticle;
import nl.tno.sensorstorm.impl.FlushingSyncBuffer;
import nl.tno.sensorstorm.impl.SyncBuffer;

public class FlushingSyncBufferTest extends TestCase {

	private static class TestDataParticle implements DataParticle {

		private final long timestamp;

		public TestDataParticle(long timestamp) {
			this.timestamp = timestamp;
		}

		@Override
		public long getTimestamp() {
			return timestamp;
		}

		@Override
		public void setTimestamp(long timestamp) {
		}

	}

	private static class TestMetaParticle extends AbstractMetaParticle {

		public TestMetaParticle(long timestamp, String originId) {
			super(timestamp);
			setOriginId(originId);
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = (prime * result) + (int) (timestamp ^ (timestamp >>> 32));
			return result;
		}

		@Override
		public boolean equalMetaParticle(MetaParticle obj) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (getClass() != obj.getClass()) {
				return false;
			}
			TestMetaParticle other = (TestMetaParticle) obj;
			if (timestamp != other.timestamp) {
				return false;
			}
			return true;
		}

	}

	public void testBufferSize() {
		SyncBuffer b = new FlushingSyncBuffer(10);
		List<Particle> l;
		// learn
		b.pushParticle(new TestMetaParticle(1, "One"));
		b.pushParticle(new TestMetaParticle(1, "Two"));
		b.pushParticle(new TestMetaParticle(1, "Three"));

		assertTrue(new TestMetaParticle(1, "One").equals(new TestMetaParticle(
				1, "two")));

		// Fill
		b.pushParticle(new TestDataParticle(1));
		b.pushParticle(new TestDataParticle(2));
		b.pushParticle(new TestDataParticle(3));

		assertTrue(new GracefullShutdownParticle(1, "One")
				.equals(new GracefullShutdownParticle(1, "Two")));

		// Trigger
		l = b.pushParticle(new GracefullShutdownParticle(1, "One"));
		assertEquals(0, l.size());
		l = b.pushParticle(new GracefullShutdownParticle(1, "Two"));
		assertEquals(0, l.size());
		// Final one, this should trigger a flush
		l = b.pushParticle(new GracefullShutdownParticle(1, "Three"));
		assertTrue(5 <= l.size());

	}

}
