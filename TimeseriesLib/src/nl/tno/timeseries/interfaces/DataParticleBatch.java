package nl.tno.timeseries.interfaces;

import java.util.ArrayList;
import java.util.Collection;

public class DataParticleBatch extends ArrayList<DataParticle> {
	private static final long serialVersionUID = 4908173149847262415L;

	public DataParticleBatch() {
		super();
	}

	public DataParticleBatch(Collection<? extends DataParticle> arg0) {
		super(arg0);
	}

	public DataParticleBatch(int arg0) {
		super(arg0);
	}

	
}
