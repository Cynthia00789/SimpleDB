package simpledb;

import java.io.*;
import java.util.HashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 */
public class BufferPool {
    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;
    public static final int pagesize =PAGE_SIZE;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    /* Structure to represent bufferpool which has pages */
    /* PageId and page are interfaces defined in PageId and Page.java */
    private HashMap<PageId,Page> bpage ;
    private int maxpage;

    /*HashTable to keep record of Least Recently Used Page */
    /*Recently added page will have priority of value 0*/
    /*Least Recently Used Page will have highest priority value and it will be evicted*/
    private HashMap<PageId, Integer> recent;


    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        bpage=  new HashMap<PageId,Page>();
        recent = new HashMap<PageId, Integer>();
        maxpage = numPages;
    }

    /*
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     *  <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
      */

    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
            // Method to get value from key in HashMap
            if(bpage.containsKey(pid)){
                updatePriority();
                recent.put(pid,0);
                return bpage.get(pid);

            }
            else if(bpage.size()>=maxpage){
                evictPage();
            }
            DbFile file = Database.getCatalog().getDbFile(pid.getTableId());  // these methods are defined in Catalog.java
            Page p = file.readPage(pid); // Method specified in DbFile.java to read the page
            bpage.put(pid,p);   // Adding page in the Bufferpool , pid and p are parameters specified in HashMap
            updatePriority();
            recent.put(pid, 0);
            return p;
            // create a new page and add


        // some code goes here

    }

    /* Method to update the priority of all pages
     * Page with highest priority is least recently used
     */
    public void updatePriority(){
        if(recent.size() == 0)
          return;
        if(recent.size() > 0){
            for (PageId iter : recent.keySet()){
                int priority = recent.get(iter);
                priority++;
                recent.put(iter,priority);
            }
         }
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
        // not necessary for proj1
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for proj1
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for proj1
        return false;
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
        // not necessary for proj1
    }

    /**
     * Add a tuple to the specified table behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to(Lock
     * acquisition is not needed for lab2). May block if the lock cannot
     * be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have
     * been dirtied so that future requests see up-to-date pages.
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for proj1
        DbFile dbfile = Database.getCatalog().getDbFile(tableId);
        Page p = dbfile.insertTuple(tid, t).get(0);
        p.markDirty(true, tid);
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from. May block if
     * the lock cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit.  Does not need to update cached versions of any pages that have
     * been dirtied, as it is not possible that a new page was created during the deletion
     * (note difference from addTuple).
     *
     * @param tid the transaction adding the tuple.
     * @param t the tuple to add
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
        // some code goes here
        // not necessary for proj1
        DbFile dbfile = Database.getCatalog().getDbFile(t.getRecordId().getPageId().getTableId());
        Page p = dbfile.deleteTuple(tid, t);
        p.markDirty(true, tid);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for proj1
        for (PageId key : bpage.keySet()) {
            flushPage(key);
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
	// not necessary for proj1
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for proj1
        Page flush = bpage.get(pid);
        DbFile file = Database.getCatalog().getDbFile(pid.getTableId());// accessing dbfile using methods from other classes
        TransactionId dirty = flush.isDirty(); // fetching the dirty transactionId
        if(dirty != null){
            file.writePage(flush);
            flush.markDirty(false, null);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for proj1

        PageId lru_pageid = null; // least recently used pageid
        int i = -1;
        //Finding page with highest priority(least recently used)
        for(PageId iter : recent.keySet()){
                int prior = recent.get(iter);
                if( prior > i){
                i = prior;
                lru_pageid = iter;
            }
        }
        //writing the page to disk if it's dirty
        try{
            flushPage(lru_pageid);
        }catch (IOException e){
            e.printStackTrace();
        }
        // removing the page from bufferpool
        bpage.remove(lru_pageid);
        // removing the pageid from recently accesed page's hashmap <v>recent<v>
        recent.remove(lru_pageid);
    }


}
 
