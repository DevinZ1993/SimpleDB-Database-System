/** @author Nan Zuo (devinz1993.github.io) */

package simpledb;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

class LogRecord {
	public final long tid;
	protected long offset;
	
	public LogRecord(RandomAccessFile raf) throws IOException {
		tid = raf.readLong();
	}
	
	public long getOffset() {
		return offset;
	}
	
	public static LogRecord readNext(RandomAccessFile raf) 
			throws IOException {
		switch (raf.readInt()) {
			case LogFile.BEGIN_RECORD:
				return new BeginRecord(raf);
			case LogFile.ABORT_RECORD:
				return new AbortRecord(raf);
			case LogFile.COMMIT_RECORD:
				return new CommitRecord(raf);
			case LogFile.UPDATE_RECORD:
				return new UpdateRecord(raf);
			case LogFile.CHECKPOINT_RECORD:
				return new CheckPointRecord(raf);
			default:
				return null;
		}
	}
}

class BeginRecord extends LogRecord {
	
	public BeginRecord(RandomAccessFile raf) throws IOException {
		super(raf);
		offset = raf.readLong();
	}
	
	@Override
	public String toString() {
		return "[BEGIN]\ttid = "+tid+"\toffset = "+offset;
	}
}

class AbortRecord extends LogRecord {
	
	public AbortRecord(RandomAccessFile raf) throws IOException {
		super(raf);
		offset = raf.readLong();
	}
	
	@Override
	public String toString() {
		return "[ABORT]\ttid = "+tid+"\toffset = "+offset;
	}
}

class CommitRecord extends LogRecord {
	
	public CommitRecord(RandomAccessFile raf) throws IOException {
		super(raf);
		offset = raf.readLong();
	}
	
	@Override
	public String toString() {
		return "[COMMIT]\ttid = "+tid+"\toffset = "+offset;
	}
}

class UpdateRecord extends LogRecord {
	public final Page beforeImage, afterImage;
	
	public UpdateRecord(RandomAccessFile raf) throws IOException {
		super(raf);
		beforeImage = Database.getLogFile().readPageData(raf);
		afterImage = Database.getLogFile().readPageData(raf);
		offset = raf.readLong();
	}

	@Override
	public String toString() {
		return "[UPDATE]\ttid = "+tid+"\toffset = "+offset+"\tpid = "+beforeImage.getId();
	}
}

class CheckPointRecord extends LogRecord {
	private Map<Long,Long> map = new HashMap<Long,Long>();
	
	public CheckPointRecord(RandomAccessFile raf) throws IOException {
		super(raf);
		int num = raf.readInt();
		
		for (int i=0; i<num; i++) {
			long key = raf.readLong();
			long val = raf.readLong();
			
			map.put(key, val);
		}
		offset = raf.readLong();
	}
	
	public Set<Long> keySet() {
		return map.keySet();
	}
	
	public boolean containsTid(long key) {
		return map.containsKey(key);
	}
	
	public long getOffset(long key) {
		return map.get(key);
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer("[CHECKPOINT]\toffset = "+offset);
		
		for (Long key : map.keySet()) {
			sb.append("\t("+key+","+map.get(key)+")");
		}
		return sb.toString();
	}
}
