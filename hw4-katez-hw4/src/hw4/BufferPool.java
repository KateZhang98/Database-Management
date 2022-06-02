package hw4;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;

import hw1.Database;
import hw1.HeapFile;
import hw1.HeapPage;
import hw1.Tuple;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool which check that the transaction has the appropriate
 * locks to read/write the page.
 */
public class BufferPool {
    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    
    private int numPage;
    private HashMap<String, HeapPage> cache = new HashMap<>();
    
    //locks
    private HashMap<String, Integer> writeLocks = new HashMap<String, Integer>();
    private HashMap<String, ArrayList<Integer>> readLocks = new HashMap<>();
    
    //records
    private HashMap<Integer, ArrayList<String>> tidPage = new HashMap();
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // your code here
    	this.numPage = numPages;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param tableId the ID of the table with the requested page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public HeapPage getPage(int tid, int tableId, int pid, Permissions perm)
        throws Exception {
    	HeapFile file = Database.getCatalog().getDbFile(tableId);
    	HeapPage page = file.readPage(pid);
    	if(page ==null) {
    		return null;
    	}
        // your code here
    	String key = String.valueOf(tableId) + "," + String.valueOf(pid);
    	ArrayList<String> pages = tidPage.getOrDefault(key, new ArrayList<String>());
    	//if butter pool is full
    	if(cache.size()==numPage && !cache.containsKey(key)) {
    		// if the page is not present in the cache
        	evictPage();
    	}
   
    	//write locks held by another transaction
    	if(writeLocks.containsKey(key)&&writeLocks.get(key)!= tid) {
    		transactionComplete(tid,false);
    		return null;
    	}
    	if(readLocks.containsKey(key)&&!readLocks.get(key).contains(tid)) {
//    		transactionComplete(tid,false);
    		return null;
    	}
    	
    	//acquire a lock and may block if that lock is held by another transaction
    	//read
    	if(perm==Permissions.READ_ONLY) {
    		ArrayList<Integer> tids = readLocks.get(key);
    		if(tids==null) {
    			tids = new ArrayList<>();
    		}
    		if(!tids.contains(tid)) {
    			tids.add(tid);
    		}
    		readLocks.put(key, tids);
    	}
    	else {
    		//write
    		writeLocks.put(key, tid);
    		
    	}
    	if(!pages.contains(key)) {
    		pages.add(key);
    	}
    	tidPage.put(tid, pages);
    	cache.put(key, page);
    	return page;
    	
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param tableID the ID of the table containing the page to unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(int tid, int tableId, int pid) {
        // your code here
    	//release = delete the pid from the lock
    	String key = String.valueOf(tableId) +","+ String.valueOf(pid);
    	//write lock
    	if(writeLocks.containsKey(key)&&writeLocks.get(key)==tid) {
    		writeLocks.remove(key);
    	}
    	//read lock
    	if(readLocks.containsKey(key)) {
    		readLocks.get(key).remove(tid);
    		if(readLocks.get(key).size()==0) {
    			readLocks.remove(key);
    		}
    	}
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(int tid, int tableId, int pid) {
        // your code here
    	String key = String.valueOf(tableId) +","+ String.valueOf(pid);
    	if(readLocks.containsKey(key)) {
    		if(readLocks.get(key).contains(tid)) {
    			return true;
    		}
    	}
    	
    	if(writeLocks.containsKey(key)&&writeLocks.get(key)==tid) {
    		return true;
    	}
    	
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction. If the transaction wishes to commit, write
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(int tid, boolean commit)
        throws IOException {
        // your code here
    	for(String key: tidPage.get(tid)) {
    		int tableId = Integer.valueOf(key.split("\\,")[0]);
    		int pid = Integer.valueOf(key.split("\\,")[1]);
    		releasePage(tid,tableId,pid);
 
    		if(cache.get(key).isDirty()) {
    			if(commit) {
    				flushPage(tableId,pid);
    			}
    			else {
    				cache.remove(key);
    			}
    		}
    	}
    }

    /**
     * Add a tuple to the specified table behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to. May block if the lock cannot 
     * be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(int tid, int tableId, Tuple t)
        throws Exception {
        // your code here
    	String key = String.valueOf(tableId) +","+ String.valueOf(t.getPid());
    	if(writeLocks.containsKey(key)&&writeLocks.get(key)==tid) {
    		HeapPage hp = cache.get(key);
    		hp.addTuple(t);
    		hp.setDirty(true);
    		cache.put(key, hp);
    	}
    	else {
    		throw new Exception();
    	}

    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from. May block if
     * the lock cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty.
     *
     * @param tid the transaction adding the tuple.
     * @param tableId the ID of the table that contains the tuple to be deleted
     * @param t the tuple to add
     */
    public void deleteTuple(int tid, int tableId, Tuple t)
        throws Exception {
        // your code here
    	String key = String.valueOf(tableId) +","+ String.valueOf(t.getPid());
    	if(writeLocks.containsKey(key)&&writeLocks.get(key)==tid) {
    		HeapPage hp = cache.get(key);
    		hp.deleteTuple(t);
    		hp.setDirty(true);
    		cache.put(key, hp);
    	}
    	else {
    		throw new Exception();
    	}
    }
    //when need to remove a page from a buffer
    //each of the pages modified by the transaction are written to disk.
    //mark each of those pages as clean (not dirty) so that they can be used by other transactions, if necessary.
    private synchronized void flushPage(int tableId, int pid) throws IOException {
        // your code here
    	String key = String.valueOf(tableId) +","+ String.valueOf(pid);
    	if(cache.containsKey(key)) {
    		if(cache.get(key).isDirty()) {
    			Database.getCatalog().getDbFile(tableId).writePage(cache.get(key));
    			cache.get(key).setDirty(false);
    		}
    	}
    	else {
    		throw new IOException();
    	}

    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     * don't want to evict pages being modified & not committed yet 
     * evict only clean page, go to the buffer pool, evict the first clean page & make space for the upcoming page
     */
    private synchronized void evictPage() throws Exception {
        // your code here
    	for(String k: cache.keySet()) {
    		HeapPage temp = cache.get(k);
    		if(temp.isDirty()) continue;
    		else {
    			cache.remove(k);
    			for(ArrayList<String> pages: tidPage.values()) {
    				pages.remove(k);
    			}
    			readLocks.remove(k);
    			writeLocks.remove(k);
    			return;
    		}
    	}
    	throw new Exception();
    }

}
