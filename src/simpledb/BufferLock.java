/**
 * @author Nan Zuo (devinz1993.github.io)
 */

package simpledb;

import java.util.Collections;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.*;


class BufferLock {
	
	private static final TransactionId NIL_TID = new TransactionId();
	private static final Random rand = new Random();
	
	private final ConcurrentMap<Integer,Set<TransactionId>> readers;
	private final ConcurrentMap<Integer,TransactionId> writers;
	private final Semaphore[] waits, mutexes, writeMutexes;
	private final AtomicInteger[] readCnts;
	
	public BufferLock(int numPages) {
		readers = new ConcurrentHashMap<Integer,Set<TransactionId>>();
		writers = new ConcurrentHashMap<Integer,TransactionId>();
		waits = new Semaphore[numPages];
		mutexes = new Semaphore[numPages];
		writeMutexes = new Semaphore[numPages];
		readCnts = new AtomicInteger[numPages];
		for  (int i=0; i<numPages; i++) {
			readers.put(i, Collections.newSetFromMap
					(new ConcurrentHashMap<TransactionId,Boolean>()));
			writers.put(i, NIL_TID);
			waits[i] = new Semaphore(1);
			mutexes[i] = new Semaphore(1);
			writeMutexes[i] = new Semaphore(1);
			readCnts[i] = new AtomicInteger(0);
		}
	}
	
	private synchronized boolean holdsReadLock(TransactionId tid, int idx) {
		return readers.get(idx).contains(tid);
	}
	
	private synchronized boolean holdsWriteLock(TransactionId tid, int idx) {
		return tid.equals(writers.get(idx));
	}
	
	public synchronized boolean holdsLock(TransactionId tid, int idx) {
		return holdsReadLock(tid, idx) || holdsWriteLock(tid, idx);
	}
	
	private void tryAcquire(TransactionId tid, Semaphore semp, Semaphore... owneds) 
			throws InterruptedException {
		if (!semp.tryAcquire(10+rand.nextInt(50), TimeUnit.MILLISECONDS)) {
			boolean deadlock = false;
			
			//System.out.println(tid+" tryAcquire for the 2nd time");
			for (int i=0; i<mutexes.length; i++) {
				if (releaseLock(tid, i)) {
					deadlock = true;
				}
			}
			if (deadlock) {
				System.err.println("DEADLOCK");
				for (Semaphore owned : owneds) {
					owned.release();
				}
				throw new InterruptedException("deadlock detected by "+tid);
			} else {
				semp.acquire();
			}
		}
	}
	
	public void getLock(TransactionId tid, int idx, Permissions perm) 
			throws InterruptedException {
		if (!holdsWriteLock(tid, idx) && !(Permissions.READ_ONLY == perm
				&& holdsReadLock(tid, idx))) {
			//System.out.println("Get "+tid+" "+idx+" "+perm);
			tryAcquire(tid, waits[idx]);
			if (Permissions.READ_ONLY == perm) {
				if (0 == readCnts[idx].getAndIncrement()) {
					tryAcquire(tid, mutexes[idx], writeMutexes[idx], waits[idx]);
				}
				readers.get(idx).add(tid);
			} else {
				tryAcquire(tid, writeMutexes[idx], waits[idx]);
				releaseLock(tid, idx);
				tryAcquire(tid, mutexes[idx], writeMutexes[idx], waits[idx]);
				writers.put(idx, tid);
			}
			waits[idx].release();
			//System.out.println("get "+tid+" "+idx+" "+perm);
		}
	}
	
	public boolean releaseLock(TransactionId tid, int idx) {
		if (holdsReadLock(tid, idx)) {
			//System.out.println("Release "+tid+" "+idx);
			readers.get(idx).remove(tid);
			if (0 == readCnts[idx].decrementAndGet()) {
				mutexes[idx].release();
			}
			//System.out.println("release "+tid+" "+idx);
			return true;
		} else if (holdsWriteLock(tid, idx)) {
			//System.out.println("Release "+tid+" "+idx);
			writers.put(idx, NIL_TID);
			mutexes[idx].release();
			writeMutexes[idx].release();
			//System.out.println("release "+tid+" "+idx);
			return true;
		} else {
			return false;
		}
	}
	
}
