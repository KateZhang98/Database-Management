package hw2;

import java.util.ArrayList;
import java.util.Arrays;

import hw1.Field;
import hw1.RelationalOperator;
import hw1.Tuple;
import hw1.TupleDesc;
import hw1.Type;

/**
 * This class provides methods to perform relational algebra operations. It will be used
 * to implement SQL queries.
 * @author Doug Shook
 *
 */
public class Relation {

	private ArrayList<Tuple> tuples;
	private TupleDesc td;
	
	public Relation(ArrayList<Tuple> l, TupleDesc td) {
		//your code here
		this.tuples=l;
		this.td = td;
	}
	
	/**
	 * This method performs a select operation on a relation
	 * @param field number (refer to TupleDesc) of the field to be compared, left side of comparison
	 * @param op the comparison operator
	 * @param operand a constant to be compared against the given column
	 * @return
	 */
	public Relation select(int field, RelationalOperator op, Field operand) {
		//your code here
		//create a new relation to store the result everytime to avoid modifying the table. 
//		if(tuples.isEmpty()) {
//			return new Relation(new ArrayList<Tuple>(),td);
//		}
		ArrayList<Tuple> tups = new ArrayList<Tuple>();
		for (Tuple tup: tuples) {
//			System.out.println("a tuple in the table " + tup);
			if(tup.getField(field).compare(op, operand)){
//				System.out.println("satisfied tuple: "+ tup.toString());
				tups.add(tup);
			}
		}
		
		return new Relation(tups,td);
	}
	
	/**
	 * This method performs a rename operation on a relation
	 * @param fields the field numbers (refer to TupleDesc) of the fields to be renamed
	 * @param names a list of new names. The order of these names is the same as the order of field numbers in the field list
	 * @return
	 * @throws Exception 
	 */
	public Relation rename(ArrayList<Integer> fields, ArrayList<String> names) throws Exception {
		//copy all type & fields
		Type[] type = td.getTypes().clone();
		String[] fname = td.getFields().clone();

		for(int i=0;i<fields.size();i++) {
			Integer pos = fields.get(i);
			String newName = names.get(i);
			if(Arrays.asList(fname).contains(newName)) {
				throw new Exception();
			}
			if(pos!=null&&newName!="") {
				fname[(int)pos] = newName;
			}
		}
		;
		TupleDesc newTd = new TupleDesc(type,fname);
		System.out.println("tupedescription after rename"+newTd.toString());
//		ArrayList<Tuple> temp = tuples;
//		for(Tuple t: temp) {
//			t.setDesc(newTd);
//		}
//		this.td = newTd;
		return new Relation(this.tuples,newTd);
	}
	
	/**
	 * This method performs a project operation on a relation
	 * @param fields a list of field numbers (refer to TupleDesc) that should be in the result
	 * @return
	 * @throws Exception 
	 */
	public Relation project(ArrayList<Integer> fields) throws IllegalArgumentException {
		//your code here
		//keep all the columns in the fields, do not edit the td directly. 
		//Create a new tuple description to store the fields we want select. 
		Type[] type = new Type[fields.size()];
		String[] fd = new String[fields.size()];
		for(int i=0;i<fields.size();i++) {
			if(fields.get(i)>this.td.numFields()-1) {
				throw new IllegalArgumentException();
			}
			type[i]=this.td.getType(fields.get(i));
			fd[i]=this.td.getFieldName(fields.get(i));
		}
		TupleDesc newTd = new TupleDesc(type,fd);
		ArrayList<Tuple> newT = new ArrayList<Tuple>();
		if(fields.size()==0) {
			return new Relation(newT,newTd);
		}
		for(Tuple tup:tuples) {
			Tuple tt= new Tuple(newTd);
			for(int i =0;i<fields.size();i++) {
				tt.setField(i, tup.getField(fields.get(i)));
			}
			newT.add(tt);
		}
		
		Relation table = new Relation(newT,newTd);
		
		return table;
	}
	
	/**
	 * This method performs a join between this relation and a second relation.
	 * The resulting relation will contain all of the columns from both of the given relations,
	 * joined using the equality operator (=)
	 * @param other the relation to be joined
	 * @param field1 the field number (refer to TupleDesc) from this relation to be used in the join condition
	 * @param field2 the field number (refer to TupleDesc) from other to be used in the join condition
	 * @return
	 */
	public Relation join(Relation other, int field1, int field2) {
		//your code here
		Type[] typeAr = new Type[this.td.numFields()+other.td.numFields()];
		String[] fieldAr = new String[this.td.numFields()+other.td.numFields()];
		for(int i =0;i<this.td.numFields();i++) {
			typeAr[i] = this.td.getType(i);
			fieldAr[i]=this.td.getFieldName(i);
		}
		for(int i=this.td.numFields();i<this.td.numFields()+other.td.numFields();i++) {
			typeAr[i] = other.td.getType(i-this.td.numFields());
			fieldAr[i]=other.td.getFieldName(i-this.td.numFields());
		}
//		this.td.setFields(fieldAr);
//		this.td.setType(typeAr);
		
		TupleDesc newTd = new TupleDesc(typeAr,fieldAr);
		ArrayList<Tuple> newTuple = new ArrayList<Tuple>();
		
 		for(Tuple t1: this.tuples) {
 			for(Tuple t2: other.tuples) {
// 				System.out.println("left tuple description:" + t1.getField(field1));
 				if(t1.getField(field1).equals(t2.getField(field2))) {
// 					System.out.println("get into the if loop");
 					Tuple t3 = new Tuple(newTd);
 					for(int i =0;i<this.getDesc().numFields();i++) {
 						t3.setField(i, t1.getField(i));
 					}
 					int index = this.getDesc().numFields();
 					for(int i =0;i<other.getDesc().numFields();i++) {
 						t3.setField(i+index, t2.getField(i));
 					}
 					newTuple.add(t3);
 				}
 			}
 		}
		return new Relation(newTuple,newTd);
	}
	
	/**
	 * Performs an aggregation operation on a relation. See the lab write up for details.
	 * @param op the aggregation operation to be performed
	 * @param groupBy whether or not a grouping should be performed
	 * @return
	 */
	public Relation aggregate(AggregateOperator op, boolean groupBy) {
		//your code here
		Aggregator agg = new Aggregator(op, groupBy, this.td);
		for(Tuple tuple: this.tuples) {
			agg.merge(tuple);
		}
		ArrayList<Tuple> newTuple = agg.getResults();
		TupleDesc newTd = newTuple.get(0).getDesc();

		return new Relation(newTuple,newTd);
	}
	
	public TupleDesc getDesc() {
		//your code here
		return td;
	}
	
	public ArrayList<Tuple> getTuples() {
		//your code here
		return tuples;
	}
	
	/**
	 * Returns a string representation of this relation. The string representation should
	 * first contain the TupleDesc, followed by each of the tuples in this relation
	 */
	public String toString() {
		//your code here
		String s = td.toString()+ '\n';
		for(Tuple tuple:this.tuples) {
			String temp = tuple.toString()+ '\n';
			s+=temp;
		}
		return s;
	}
}
