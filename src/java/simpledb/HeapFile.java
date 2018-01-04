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

    private File f;

    private TupleDesc td;


    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
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
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return f.getAbsoluteFile().hashCode();
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
        final int pageSize = Database.getBufferPool().getPageSize();
        byte[] data = new byte[pageSize];
        try {
            FileInputStream fis = new FileInputStream(f);
            fis.skip(pid.getPageNumber() * pageSize);
            fis.read(data); //read file into bytes[]
            fis.close();
            return new HeapPage(new HeapPageId(pid.getTableId(), pid.getPageNumber()), data);
        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException("HeapFile.readPage: FileNotFoundException");
        } catch (IOException e) {
            throw new IllegalArgumentException("HeapFile.readPage: IOException");
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        final int pageSize = Database.getBufferPool().getPageSize();
        RandomAccessFile writer = new RandomAccessFile (f, "rw");
        writer.skipBytes(page.getId().getPageNumber() * pageSize);
        writer.write(page.getPageData());
        writer.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        final int pageSize = Database.getBufferPool().getPageSize();
        return (int)(f.length() / pageSize);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        final ArrayList<Page> modifiedPages = new ArrayList<>();
        final int numPages = numPages();
        HeapPage page = null;
        int pgNo = 0;

        // find available page
        while (pgNo < numPages)
        {
            HeapPageId pageId = new HeapPageId(getId(), pgNo);
            HeapPage tmp = (HeapPage)Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
            if(tmp.getNumEmptySlots() > 0) {
                page = tmp;
                break;
            }
            pgNo ++;
        }

        // not found
        if(page == null) {
            // create a new page
            HeapPageId pageId = new HeapPageId(getId(), pgNo);
            page = new HeapPage(pageId, HeapPage.createEmptyPageData());
            // write to disk
            writePage(page);
            // use cache from bufferpoll
            page = (HeapPage)Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
        }

        // insert
        page.insertTuple(t);

        // return
        return new ArrayList<>(Arrays.asList(page));
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);
        t.setRecordId(new RecordId(new HeapPageId(-1, -1), -1));
        return new ArrayList<>(Arrays.asList(page));
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new DbFileIterator() {

            private int currPgNo;
            private Iterator<Tuple> iterator;
            
            @Override
            public void open() throws DbException, TransactionAbortedException {
                rewind();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if(iterator == null)
                    return false;

                if(currPgNo + 1 >= numPages() && !iterator.hasNext())
                    return false;
                return true;
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if(iterator == null)
                    throw new NoSuchElementException();

                if(iterator.hasNext())
                    return iterator.next();

                while (++currPgNo < numPages())
                {
                    iterator = getPage().iterator();
                    if(iterator.hasNext())
                        return iterator.next();
                }

                return null;
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                currPgNo = 0;
                iterator = getPage().iterator();
            }

            @Override
            public void close() {
                currPgNo = 0;
                iterator = null;
            }

            private HeapPage getPage() throws TransactionAbortedException, DbException {
                return (HeapPage)Database.getBufferPool().getPage(tid, new HeapPageId(getId(), currPgNo), Permissions.READ_ONLY);
            }

        };
    }

}

