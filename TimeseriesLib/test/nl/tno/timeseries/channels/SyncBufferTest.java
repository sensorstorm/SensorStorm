package nl.tno.timeseries.channels;

import java.util.List;

import junit.framework.TestCase;
import nl.tno.timeseries.operations.SyncBuffer;
import nl.tno.timeseries.particles.DataParticle;
import nl.tno.timeseries.particles.MetaParticle;
import nl.tno.timeseries.particles.Particle;

public class SyncBufferTest extends TestCase {

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

	private static class TestMetaParticle implements MetaParticle {

		private final long timestamp;

		public TestMetaParticle(long timestamp) {
			this.timestamp = timestamp;
		}

		@Override
		public long getTimestamp() {
			return timestamp;
		}

		@Override
		public void setTimestamp(long timestamp) {
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			TestMetaParticle other = (TestMetaParticle) obj;
			if (timestamp != other.timestamp)
				return false;
			return true;
		}

		@Override
		public String getOriginId() {
			return null;
		}

		@Override
		public void setOriginId(String originId) {
		}
	}

	public void testOrderingFlush() {
		SyncBuffer b = new SyncBuffer(10);
		b.pushParticle(new TestDataParticle(2));
		b.pushParticle(new TestDataParticle(3));
		b.pushParticle(new TestDataParticle(1));

		assertEquals(3, b.size());

		List<Particle> flush = b.flush();

		assertEquals(1, flush.get(0).getTimestamp());
		assertEquals(2, flush.get(1).getTimestamp());
		assertEquals(3, flush.get(2).getTimestamp());
	}

	public void testBufferSize() {
		SyncBuffer b = new SyncBuffer(3);
		List<Particle> l;
		l = b.pushParticle(new TestDataParticle(1));
		assertEquals(0, l.size());

		l = b.pushParticle(new TestDataParticle(2));
		assertEquals(0, l.size());

		l = b.pushParticle(new TestDataParticle(3));
		assertEquals(0, l.size());

		l = b.pushParticle(new TestDataParticle(5));
		assertEquals(2, l.size());
		assertEquals(1, l.get(0).getTimestamp());
		assertEquals(2, l.get(1).getTimestamp());

		l = b.pushParticle(new TestDataParticle(6));
		assertEquals(1, l.size());
		assertEquals(3, l.get(0).getTimestamp());

		l = b.pushParticle(new TestDataParticle(4));
		assertEquals(0, l.size());

		l = b.pushParticle(new TestDataParticle(7));
		assertEquals(1, l.size());
		assertEquals(4, l.get(0).getTimestamp());

		assertEquals(3, b.size());

		List<Particle> flush = b.flush();

		assertEquals(5, flush.get(0).getTimestamp());
		assertEquals(6, flush.get(1).getTimestamp());
		assertEquals(7, flush.get(2).getTimestamp());
	}

	public void testDuplicateMetaParticle() {
		SyncBuffer b = new SyncBuffer(10);
		b.pushParticle(new TestMetaParticle(5));
		b.pushParticle(new TestMetaParticle(5));
		b.pushParticle(new TestMetaParticle(5));

		assertEquals(1, b.size());
	}

	public void testDuplicateDataParticle() {
		SyncBuffer b = new SyncBuffer(10);
		b.pushParticle(new TestDataParticle(5));
		b.pushParticle(new TestDataParticle(5));
		b.pushParticle(new TestDataParticle(5));

		assertEquals(3, b.size());
	}

	public void testReject() {
		SyncBuffer b = new SyncBuffer(10);
		List<Particle> l;

		l = b.pushParticle(new TestDataParticle(1));
		assertEquals(0, l.size());

		l = b.pushParticle(new TestDataParticle(100));
		assertEquals(1, l.size());
		assertEquals(1, l.get(0).getTimestamp());

		l = b.pushParticle(new TestDataParticle(200));
		assertEquals(1, l.size());
		assertEquals(100, l.get(0).getTimestamp());

		try {
			// This should fail, timestamp 100 was already sent out
			b.pushParticle(new TestDataParticle(2));
			fail();
		} catch (Exception e) {
		}

	}
}
