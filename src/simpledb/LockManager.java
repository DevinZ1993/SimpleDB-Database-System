/** @author Nan Zuo (devinz1993.github.io) */

package simpledb;

import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Vector;


class LockManager {
	
	private final Object[] mutexes;
	private final List<Set<TransactionId>> readLockHolders;
	private final List<TransactionId> writeLockHolders;
	private final int[] writers;	/* number of writers waiting or working */
	private final Random rand = new Random();
	private final int MIN_TIME = 100, MAX_TIME = 1000;
	
	public LockManager(int numPages) {
		mutexes = new Object[numPages];
		readLockHolders = new Vector<Set<TransactionId>>(numPages);
		writeLockHolders = new Vector<TransactionId>(numPages);
		for (int i=0; i<numPages; i++) {
			mutexes[i] = new Object();
			readLockHolders.add(new HashSet<TransactionId>());
			writeLockHolders.add(null);
		}
		writers = new int[numPages];
	}
	
	private boolean holdsReadLock(TransactionId tid, int idx) {
		synchronized(mutexes[idx]) {
			return readLockHolders.get(idx).contains(tid);
		}
	}
	
	private boolean holdsWriteLock(TransactionId tid, int idx) {
		synchronized(mutexes[idx]) {
			return tid.equals(writeLockHolders.get(idx));
		}
	}
	
	public boolean isHolding(TransactionId tid, int idx) {
		return holdsWriteLock(tid, idx) || holdsReadLock(tid, idx);
	}
	
	private void acquireReadLock(TransactionId tid, int idx)
			throws InterruptedException {
		// System.out.println(tid.myid+" wants to read "+idx);
		if (!isHolding(tid, idx)) {
			synchronized(mutexes[idx]) {
				final Thread thread = Thread.currentThread();
				final Timer timer = new Timer(true);
				
				timer.schedule(new TimerTask() {
					@Override public void run() {
						thread.interrupt();
					}
				}, MIN_TIME+rand.nextInt(MAX_TIME-MIN_TIME));
				while (0 != writers[idx]) {
					mutexes[idx].wait();
				}
				readLockHolders.get(idx).add(tid);
				timer.cancel();
			}
		}
		// System.out.println(tid.myid+" gets to read "+idx);
	}
	
	private boolean releaseReadLock(TransactionId tid, int idx) {
		if (!holdsReadLock(tid, idx)) {
			return false;
		} else {
			synchronized(mutexes[idx]) {
				readLockHolders.get(idx).remove(tid);
				if (readLockHolders.get(idx).isEmpty()) {
					mutexes[idx].notifyAll();
				}
			}
			// System.out.println(tid.myid+" ceases to read "+idx);
			return true;
		}
	}
	
	private boolean hasOtherReader(TransactionId tid, int idx) {
		synchronized(mutexes[idx]) {
			for (TransactionId otherTid : readLockHolders.get(idx)) {
				if (!otherTid.equals(tid)) {
					return true;
				}
			}
			return false;
		}
	}
	
	private void acquireWriteLock(TransactionId tid, int idx) 
			throws InterruptedException {
		// System.out.println(tid.myid+" wants to write "+idx);
		if (!holdsWriteLock(tid, idx)) {
			synchronized(mutexes[idx]) {
				final Thread thread = Thread.currentThread();
				final Timer timer = new Timer(true);
				
				writers[idx]++;
				timer.schedule(new TimerTask() {
					@Override public void run() {
						thread.interrupt();
					}
				}, MIN_TIME+rand.nextInt(MAX_TIME-MIN_TIME));
				while (hasOtherReader(tid, idx) || null != writeLockHolders.get(idx)) {
					mutexes[idx].wait();
				}
				readLockHolders.get(idx).remove(tid);
				writeLockHolders.set(idx, tid);
				timer.cancel();
			}
		}
		// System.out.println(tid.myid+" gets to write "+idx);
	}
	
	private boolean releaseWriteLock(TransactionId tid, int idx) {
		if (!holdsWriteLock(tid, idx)) {
			return false;
		} else {
			synchronized(mutexes[idx]) {
				writeLockHolders.set(idx, null);
				writers[idx]--;
				mutexes[idx].notifyAll();
			}
			// System.out.println(tid.myid+" ceases to write "+idx);
			return true;
		}
	}
	
	public void acquire(TransactionId tid, int idx, Permissions perm) 
			throws InterruptedException {
		try {
			if (perm.equals(Permissions.READ_ONLY)) {
				acquireReadLock(tid, idx);
			} else {
				acquireWriteLock(tid, idx);
			}
			return;
		} catch (InterruptedException e) {
			for (int j=0; j<mutexes.length; j++) {
				release(tid, j);
			}
			if (!perm.equals(Permissions.READ_ONLY)) {
				synchronized(mutexes[idx]) {
					writers[idx]--;
				}
			}
			throw new InterruptedException("DEADLOCK DETECTED");
		}
	}
	
	public boolean release(TransactionId tid, int idx) {
		return releaseWriteLock(tid, idx) || releaseReadLock(tid, idx);
	}
	
}
