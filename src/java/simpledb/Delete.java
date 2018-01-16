package simpledb;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId t;
    private OpIterator child;

    private List<Tuple> results;
    private Iterator<Tuple> it;
    private int affectedTuplesCount = 0;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
        this.t = t;
        this.child = child;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return new TupleDesc(new Type[]{Type.INT_TYPE});
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        child.open();
        try {
            while (child.hasNext()) {
                Database.getBufferPool().deleteTuple(t, child.next());
                affectedTuplesCount ++;
            }
        } catch (IOException e) {
            throw new TransactionAbortedException();
        }

        Tuple result = new Tuple(getTupleDesc());
        result.setField(0, new IntField(affectedTuplesCount));
        results = Arrays.asList(result);
        it = results.iterator();
        super.open();
    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
        it = null;
        affectedTuplesCount = 0;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        it = results.iterator();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (it != null && it.hasNext()) {
            return it.next();
        } else
            return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        child = children[0];
    }

}
