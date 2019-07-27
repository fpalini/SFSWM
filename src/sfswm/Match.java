package sfswm;

import com.google.common.base.Objects;

public class Match {
	
	public int id1, id2;
	private int pos1, pos2;
	public int mismatch;
	
	public Match(int id1, int id2, int pos1, int pos2, int mismatch) {
		this.id1 = id1;
		this.id2 = id2;
		this.pos1 = pos1;
		this.pos2 = pos2;
		this.mismatch = mismatch;
	}

	@Override
	public boolean equals(Object obj) {
		if (!obj.getClass().equals(Match.class)) return false;
		
		Match m = (Match) obj;
		
		return id1 == m.id1 && id2 == m.id2 && (pos1 == m.pos1 || pos2 == m.pos2);
	}
	
	@Override
	public int hashCode() {
		return Objects.hashCode(id1, id2);
	}
}
