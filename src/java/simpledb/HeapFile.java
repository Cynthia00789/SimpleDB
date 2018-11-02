package simpledb;

import java.io.*;
import java.util.*;
/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    File f;
    TupleDesc td;
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f=f;
        this.td=td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        int hashc=f.getAbsoluteFile().hashCode();

        return hashc;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int offset = pid.pageNumber()*BufferPool.PAGE_SIZE;
        int size = BufferPool.PAGE_SIZE;
        byte[] buf = new byte[size];
        Page p;
        try{
            RandomAccessFile raf = new RandomAccessFile(f, "r");
            raf.seek(offset);
            raf.read(buf, 0, size);
            p = new HeapPage((HeapPageId)pid, buf);
            raf.close();
            return p;
        } catch (Exception e){
            System.out.println("RandomAccessFile couldnt open");
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for proj1
        try {
         RandomAccessFile raf = new RandomAccessFile(f, "rw");
         PageId pid = page.getId();
         int offset = pid.pageNumber()*BufferPool.PAGE_SIZE;
         byte[] buf = page.getPageData();
         raf.seek(offset);
         raf.write(buf, 0, BufferPool.PAGE_SIZE);
         raf.close();

      } catch (IOException ex) {
         ex.printStackTrace();
      }

    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        double filelen=f.length();
        double num=Math.ceil(filelen/BufferPool.PAGE_SIZE);
        return (int)num;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        //return null;
        // not necessary for proj1
        ArrayList<Page> pageList = new ArrayList<Page>();
       int tableid=this.getId();
       for (int i=0; i<this.numPages();i++){
           HeapPageId pid= new HeapPageId(tableid,i);
           Page page = Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
           if (((HeapPage)page).getNumEmptySlots()!=0){
               pageList.add(page);
               break;
           }
       }

       if(pageList.size()==0)
       {
           HeapPageId pid=new HeapPageId(this.getId(), this.numPages());
           HeapPage hp = new HeapPage(pid, HeapPage.createEmptyPageData());
           hp.insertTuple(t);
           this.writePage(hp);
           pageList.add(hp);
       }
       else
       {
           Page p = pageList.get(0);
           HeapPage hp=(HeapPage)p;
           hp.insertTuple(t);
           pageList.add(hp);
       }

       return pageList;


    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        //return null;
        // not necessary for proj1
        if((t.getRecordId()==null)||(getId() != t.getRecordId().getPageId().getTableId()))
              throw new DbException("tuple cannot be found");

        HeapPage hp = (HeapPage)Database.getBufferPool().getPage(tid,t.getRecordId().getPageId(),Permissions.READ_WRITE);
        hp.deleteTuple(t);
        return hp;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid,this);
    }

}

class HeapFileIterator implements DbFileIterator{
  private Iterator<Tuple> tupIt = null;
  private TransactionId tid = null;
  private HeapFile f = null;
  private int pgNo = 0;

  public HeapFileIterator(TransactionId tid , HeapFile f){
    this.tid = tid;
    this.f = f;
  }

  public void open() throws DbException, TransactionAbortedException{
    pgNo = 0;
    PageId pgId = new HeapPageId(f.getId(), pgNo);
    Page page = Database.getBufferPool().getPage(tid,pgId,Permissions.READ_ONLY);
    HeapPage heapPage = (HeapPage)page;
    /*if(heapPage == null)
    System.out.println("*****");
    if(page ==null)
    System.out.println("####");
    if(pgId == null)
    System.out.println("$$$$");*/
    tupIt = heapPage.iterator();

  }

    public boolean hasNext()  throws DbException, TransactionAbortedException{
        if(tupIt==null)
            return false;

        if(tupIt.hasNext())
            return true;
        else{
            if(pgNo>=f.numPages()-1)
                return false;
            else{
                PageId pgId = new HeapPageId(f.getId(), pgNo+1);
                HeapPage heappage = (HeapPage)Database.getBufferPool().getPage(tid,pgId,Permissions.READ_ONLY);

                return heappage.iterator().hasNext();
            }
        }
    }


    public Tuple next()  throws DbException, TransactionAbortedException, NoSuchElementException{
        if(tupIt==null)
            throw new NoSuchElementException();

        if (tupIt.hasNext())
            return tupIt.next();
        else
        {
            PageId pgId = new HeapPageId(f.getId(), pgNo+1);
            Page page = Database.getBufferPool().getPage(tid,pgId,Permissions.READ_ONLY);
            HeapPage heappage = (HeapPage)page;

            if (page!=null)
                if (heappage.iterator().hasNext())
                {
                    pgNo++;
                    tupIt = heappage.iterator();
                    return tupIt.next();
                }
            throw new NoSuchElementException();

        }


    }


    public void rewind() throws DbException, TransactionAbortedException{
        close();
        open();

    }


    public void close(){
        tupIt = null;
        pgNo = 0;
    }
}
