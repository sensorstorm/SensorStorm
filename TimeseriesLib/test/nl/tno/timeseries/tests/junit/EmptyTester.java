package nl.tno.timeseries.tests.junit;

import junit.framework.TestCase;

public class EmptyTester extends TestCase {

	@Override
	protected void setUp() {
		System.out.println("Setup");
	}

	@Override
	protected void tearDown() {
		System.out.println("tearDown");

	}

	public void testOperation() {
		System.out.println("test");
	}

}
