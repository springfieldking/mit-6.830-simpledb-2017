package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private TupleDesc td;

    private Map<Field, Integer> groups = new HashMap<>();

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if(what != Op.COUNT)
            throw new IllegalArgumentException("only supports COUNT");

        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field groupField = tup.getField(gbfield);
        Integer count = groups.get(groupField);
        if(count == null) {
            count = 0;
            groups.put(groupField, count);
        }

        count ++;
        groups.put(groupField, count);


        if(td == null)
            td = tup.getTupleDesc();
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new OpIterator() {

            private List<Tuple> aggregateTuples = new ArrayList<>();
            private Iterator<Tuple> it;
            private TupleDesc aggregateTupleDesc;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                aggregateTupleDesc = getTupleDesc();
                for(Field key : groups.keySet()) {
                    Tuple aggTup = new Tuple(aggregateTupleDesc);
                    int count = groups.get(key);
                    aggTup.setField(0, key);
                    aggTup.setField(1, new IntField(count));
                    aggregateTuples.add(aggTup);
                }
                it = aggregateTuples.iterator();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                return it.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                return it.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                it = aggregateTuples.iterator();
            }

            @Override
            public TupleDesc getTupleDesc() {
                return new TupleDesc(new Type[]{td.getFieldType(gbfield), Type.INT_TYPE});
            }

            @Override
            public void close() {
                it = null;
                aggregateTupleDesc = null;
                aggregateTuples.clear();
            }
        };
    }

}
