package simpledb;

import java.io.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {

    private static class LRUCache <K, V> extends LinkedHashMap<K, V> {
        private int cacheSize;

        public LRUCache()
        {
            super(0, 0.75f, true);
            this.cacheSize = Integer.MAX_VALUE;
        }

        public LRUCache(int cacheSize) {
            super(cacheSize, 0.75f, true);
            this.cacheSize = cacheSize;
        }

        public int getCacheSize() {
            return cacheSize;
        }

        public void setCacheSize(int cacheSize) {
            this.cacheSize = cacheSize;
        }

        protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
            return size() > cacheSize;
        }
    }

    private static class ReadWriteLock {
        private int reader;
        private int writer;

        private Map<TransactionId, Integer> readingTransactions = new HashMap<>();
        private TransactionId writingTid;

        synchronized public void acquireReadLock(TransactionId tid) throws TransactionAbortedException {
            try{
                while(!canGrantReadAccess(tid)) {
                    wait();
                }
                reader ++ ;
                Integer count = readingTransactions.get(tid);
                if(count == null) count = 0;
                readingTransactions.put(tid, ++count);
            } catch (InterruptedException e) {
                throw new TransactionAbortedException();
            }
        }

        synchronized public void releaseReadLock(TransactionId tid) {
            reader --;
            Integer count = readingTransactions.remove(tid);
            if(count != null && count > 0) {
                readingTransactions.put(tid, --count);
            }
            notifyAll();
        }

        synchronized public void acquireWriteLock(TransactionId tid) throws TransactionAbortedException {
            try{
                while (!canGrantWriteAccess(tid)) {
                    wait();
                }
                writingTid = tid;
                writer ++;
            } catch (InterruptedException e) {
                throw new TransactionAbortedException();
            }
        }

        synchronized public void releaseWriteLock() {
            writingTid = null;
            writer --;
            notifyAll();
        }

        private boolean isOnlyReader(TransactionId tid) {
            if(reader == 1 && readingTransactions.containsKey(tid))
                return true;
            return false;
        }

        private boolean canGrantReadAccess(TransactionId tid) {
            if(tid.equals(writingTid)) return true;
            if(writer > 0) return false;
            return true;
        }

        private boolean canGrantWriteAccess(TransactionId tid) {
            if(isOnlyReader(tid)) return true;
            if(reader > 0) return false;
            if(writingTid == null) return true;
            if(writingTid.equals(tid)) return true;
            return true;
        }
    }

    private static class TransactionLockInfo {
        private Map<PageId, List<Permissions>> oplist = new HashMap<>();

        public Map<PageId, List<Permissions>> getOplist() {
            return oplist;
        }
    }

    private LRUCache<PageId, Page> pageCache;
    private Map<PageId, ReadWriteLock>                  pageLocks       = new ConcurrentHashMap<>();
    private Map<TransactionId, TransactionLockInfo>     transactions    = new ConcurrentHashMap<>();

    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        pageCache = new LRUCache<>(numPages);
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        // get pagelock
        ReadWriteLock rwlock = pageLocks.get(pid);
        if(rwlock == null) {
            synchronized (pid) {
                rwlock = pageLocks.get(tid);
                if(rwlock == null) {
                    rwlock = new ReadWriteLock();
                    pageLocks.put(pid, rwlock);
                }
            }
        }

        // acquire lock
        if(perm == Permissions.READ_ONLY ) {
            rwlock.acquireReadLock(tid);
        } else {
            rwlock.acquireWriteLock(tid);
        }

        // get lockinfo of transactions
        TransactionLockInfo lockInfo = transactions.get(tid);
        if(lockInfo == null) {
            synchronized (tid) {
                lockInfo = transactions.get(tid);
                if(lockInfo == null) {
                    lockInfo = new TransactionLockInfo();
                    transactions.put(tid, lockInfo);
                }
            }
        }

        // add op of pid to lockinfo
        synchronized (lockInfo) {
            List<Permissions> permissions = lockInfo.oplist.get(pid);
            if(permissions == null) {
                permissions = new ArrayList<>();
                lockInfo.getOplist().put(pid, permissions);
            }
            permissions.add(perm);
        }

        // get pages
        Page page = pageCache.get(pid);
        if(page == null) {
            page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
            pageCache.put(pid, page);
        }
        return page;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2

        // get the permissions of pid and remove pid from lockinfo
        List<Permissions> ops;
        TransactionLockInfo lockInfo = transactions.get(tid);
        synchronized (lockInfo) {
            ops = lockInfo.getOplist().remove(pid);
        }

        // unlock
        if(ops == null) return;
        ReadWriteLock rwLock = pageLocks.get(pid);
        for(Permissions p : ops) {
            if(p == Permissions.READ_ONLY) {
                rwLock.releaseReadLock(tid);
            } else {
                rwLock.releaseWriteLock();
            }
        }
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        TransactionLockInfo lockInfo = transactions.get(tid);
        synchronized (lockInfo) {
            return lockInfo.getOplist().containsKey(p);
        }
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        List<Page> dirtyPages = Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid, t);
        for(Page page : dirtyPages) {
            page.markDirty(true, tid);
            pageCache.put(page.getId(), page);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        int tableId = t.getRecordId().getPageId().getTableId();
        List<Page> dirtyPages = Database.getCatalog().getDatabaseFile(tableId).deleteTuple(tid, t);
        for(Page page : dirtyPages) {
            page.markDirty(true, tid);
            pageCache.put(page.getId(), page);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        pageCache.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page page = pageCache.get(pid);
        if(page != null) {
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        Page page = null;
        try {
            if(page != null) {
                flushPage(page.getId());
                discardPage(page.getId());
            }
        } catch (IOException e) {
            throw new DbException("BufferPool.evictPage IOException");
        }
    }

}
