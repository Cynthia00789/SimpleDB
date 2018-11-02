package simpledb;
import java.util.*;
import java.lang.*;
/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    private int mgbfield;
    private int mafield;
    private Type mgbfieldtype;
    private Op mop;

    // Since gfield = null wont have two parameters we use different data structes

    HashMap<Field,ArrayList<Field>> withgrouping;
    ArrayList<Field> nogrouping;


    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        mgbfield=gbfield;
        mgbfieldtype=gbfieldtype;
        mafield = afield;
        mop=what;
        if(mgbfield==Aggregator.NO_GROUPING)
            nogrouping=new ArrayList<Field>();
        else
            withgrouping= new HashMap<Field,ArrayList<Field>>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if(mgbfield==Aggregator.NO_GROUPING)
            nogrouping.add(tup.getField(mafield));
        else{
            Field grouvalue = tup.getField(mgbfield);
            Field aggregatevalue = tup.getField(mafield);
            ArrayList<Field> agglist = withgrouping.get(grouvalue);
            if(agglist==null)
            {
                agglist = new ArrayList<Field>();
                agglist.add(aggregatevalue);
            } 
            else
                agglist.add(aggregatevalue);
            withgrouping.put(grouvalue,agglist);     
            }
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        if(mgbfield==Aggregator.NO_GROUPING){
            int result = nogrouping.size();
            TupleDesc desc = new TupleDesc(new Type[]{Type.INT_TYPE});
            Tuple t = new Tuple(desc);
            t.setField(0, new IntField(result));
            ArrayList<Tuple> finalans = new ArrayList<Tuple>();
            finalans.add(t);
            return new TupleIterator(desc,finalans); 
        }
        else{
            ArrayList<Tuple> finalans = new ArrayList<Tuple>();
            TupleDesc desc = new TupleDesc(new Type[]{mgbfieldtype, Type.INT_TYPE});

            Collection<Field> keys = withgrouping.keySet();
            Iterator<Field> keyiter = keys.iterator();
            while(keyiter.hasNext()){
                Field val = keyiter.next();
                ArrayList<Field> valuesList = withgrouping.get(val);
                int aggResult = valuesList.size();

                Tuple t = new Tuple(desc);
                t.setField(0, val);
                t.setField(1, new IntField(aggResult));
                finalans.add(t);
            }

            return new TupleIterator(desc,finalans);

        }
        
}
}
