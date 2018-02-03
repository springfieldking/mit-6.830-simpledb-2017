package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private TupleDesc td;

    private Map<Field, List<Tuple>> groups = new HashMap<>();

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field groupField;
        if(this.gbfieldtype != null) {
            groupField = tup.getField(gbfield);
        } else {
            groupField = null;
        }

        List<Tuple> tuples = groups.get(groupField);
        if(tuples == null) {
            tuples = new ArrayList<>();
            groups.put(groupField, tuples);
        }

        tuples.add(tup);

        if(td == null)
            td = tup.getTupleDesc();
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
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
                    Tuple aggTup = createTuple(key, groups.get(key));
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
                if(gbfieldtype != null)
                    return new TupleDesc(new Type[]{td.getFieldType(gbfield), Type.INT_TYPE});
                return new TupleDesc(new Type[]{Type.INT_TYPE});
            }

            @Override
            public void close() {
                it = null;
                aggregateTupleDesc = null;
                aggregateTuples.clear();
            }

            private Tuple createTuple(Field key, List<Tuple> groupTuples) {
                List<Field> fields = new ArrayList<>();
                for(Tuple tuple : groupTuples) {
                    fields.add(tuple.getField(afield));
                }

                Tuple appTup = new Tuple(aggregateTupleDesc);
                int index = 0;
                if(key != null) {
                    appTup.setField(index++, key);
                }

                int aggregateVal = calculate(fields);
                appTup.setField(index++, new IntField(aggregateVal));

                return appTup;
            }

            private int calculate(List<Field> fields) {
                boolean init = false;
                int val = 0;
                for(Field field : fields) {
                    IntField intField = (IntField) field;
                    switch (what) {
                        case COUNT:
                        {
                            val ++;
                            break;
                        }
                        case SUM:
                        {
                            val += intField.getValue();
                            break;
                        }
                        case AVG:
                        {
                            val += intField.getValue();
                            break;
                        }
                        case MIN:
                        {
                            if(!init){
                                val = intField.getValue();
                                init = true;
                            }
                            val = val<intField.getValue()?val:intField.getValue();
                            break;
                        }
                        case MAX:
                        {
                            if(!init){
                                val = intField.getValue();
                                init = true;
                            }
                            val = val>intField.getValue()?val:intField.getValue();
                            break;
                        }
                        default:
                    }
                }

                if(what == Op.AVG) {
                    val = val / fields.size();
                }

                return val;
            }
        };
    }

}
