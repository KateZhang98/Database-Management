package hw2;

import java.util.ArrayList;

import hw1.Field;
import hw1.IntField;
import hw1.StringField;
import java.util.HashMap;
import java.util.Iterator;
import hw1.RelationalOperator;

import hw1.Tuple;
import hw1.TupleDesc;
import hw1.Type;

/**
 * A class to perform various aggregations, by accepting one tuple at a time
 * @author Doug Shook
 *
 */
public class Aggregator {
	
	private AggregateOperator aggOp;
	private boolean gb;
	private TupleDesc td;
	
	private ArrayList<Tuple> tuples;

	public Aggregator(AggregateOperator o, boolean groupBy, TupleDesc td) {
		//your code here
		aggOp =o;
		gb = groupBy;
		this.td =td;
		tuples=new  ArrayList<Tuple>();
	}

	/**
	 * Merges the given tuple into the current aggregation
	 * @param t the tuple to be aggregated
	 */
	public void merge(Tuple t) {
		switch(aggOp) {
		case MIN:
			if(!gb) {
				// grouby = false
				if(tuples.isEmpty()) {
					tuples.add(t);
				}
				else {
					if(t.getField(0).compare(RelationalOperator.LT, tuples.get(0).getField(0))) {
						tuples.clear();
						tuples.add(t);
					}
				}
			}
			else {
				tuples.add(t);
			}
			break;
		case MAX:
			if(!gb) {
				if(tuples.isEmpty()) {
					tuples.add(t);
				}
				else {
					if(t.getField(0).compare(RelationalOperator.GT, tuples.get(0).getField(0))) {
						tuples.clear();
						tuples.add(t);
					}
				}
			}
			else {
				tuples.add(t);
			}
			break;
		case COUNT:
			tuples.add(t);
			break;
		case SUM:
			tuples.add(t);
			break;
		case AVG:
			tuples.add(t);
			break;
		}
	}
	
	/**
	 * Returns the result of the aggregation
	 * @return a list containing the tuples after aggregation
	 */
	public ArrayList<Tuple> getResults() {
		//your code here
		switch(aggOp) {
		case MIN:
			if(gb) {
				return groupByMinMax(RelationalOperator.LT);
			}
			break;
		case MAX:
			if(gb) {
				return groupByMinMax(RelationalOperator.GT);
			}
			break;
		case COUNT:
			if(gb) {
				ArrayList<Tuple> result = new ArrayList<Tuple>();
				
				//set up Tuple Desc
				Type[] type = new Type[] {this.td.getType(0),this.td.getType(1)};
				String[] fields = new String[] {this.td.getFieldName(0), "Count"};
				TupleDesc newTd = new TupleDesc(type,fields);
				
				//count in each group
				if(this.td.getType(0)==Type.INT) {
					HashMap<Integer,Integer> groups = new HashMap<Integer,Integer>();
					for(Tuple tup: tuples) {
						if(groups.containsKey(tup.getField(0).hashCode())){
							Tuple temp = result.get(groups.get(tup.getField(0).hashCode()));
							int count = temp.getField(1).hashCode()+1;
							temp.setField(1, new IntField(count));
						}
						else {
							Tuple newT = new Tuple(newTd);
							newT.setField(0, tup.getField(0));
							newT.setField(1, new IntField(1));
							result.add(newT);
							groups.put(newT.getField(0).hashCode(), result.size() - 1);
						}
					}
				}
				if(this.td.getType(0)==Type.STRING) {
					HashMap<String,Integer> groups = new HashMap<String,Integer>();
					
					for(Tuple tup: tuples) {
						if(groups.containsKey(tup.getField(0).toString())){
							Tuple temp = result.get(groups.get(tup.getField(0).toString()));
							int count = temp.getField(1).hashCode()+1;
							
							temp.setField(1, new IntField(count));
						}
						else {
							Tuple newT = new Tuple(newTd);
							newT.setField(0, tup.getField(0));
							newT.setField(1, new IntField(1));
							result.add(newT);
							groups.put(newT.getField(0).toString(), result.size() - 1);
						}
					}
				}
				return result;
			}
			else {
				Type[] type = new Type[] {Type.INT};
				String[] fields = new String[] {"Count"};
				TupleDesc newTd = new TupleDesc(type,fields);
				
				IntField count = new IntField(tuples.size());
				Tuple newT = new Tuple(newTd);
				newT.setField(0, count);
				tuples.clear();
				tuples.add(newT);
			}
			break;
		case SUM:
			if(gb) {
//				System.out.println("tuples in the table: "+ tuples.toString());
				ArrayList<Tuple> result = new ArrayList<Tuple>();
				
				//set up Tuple Desc
				Type[] type = new Type[] {this.td.getType(0),this.td.getType(1)};
				String[] fields = new String[] {this.td.getFieldName(0), "Sum"};
				TupleDesc newTd = new TupleDesc(type,fields);
				
				//count in each group
				if(this.td.getType(1)==Type.INT) {
					if(this.td.getType(0)==Type.INT) {
						HashMap<Integer,Integer> groups = new HashMap<Integer,Integer>();
						for(Tuple tup: tuples) {
							if(groups.containsKey(tup.getField(0).hashCode())){
								Tuple temp = result.get(groups.get(tup.getField(0).hashCode()));
								int sums = temp.getField(1).hashCode()+tup.getField(1).hashCode();
//								System.out.println("sums calculation"+sums);
								temp.setField(1, new IntField(sums));
							}
							else {
								Tuple newT = new Tuple(newTd);
								newT.setField(0, tup.getField(0));
								newT.setField(1, tup.getField(1));
								result.add(newT);
								groups.put(newT.getField(0).hashCode(), result.size() - 1);
							}
						}
					}
					if(this.td.getType(0)==Type.STRING) {
						HashMap<String,Integer> groups = new HashMap<String,Integer>();
						
						for(Tuple tup: tuples) {
							if(groups.containsKey(tup.getField(0).toString())){
								Tuple temp = result.get(groups.get(tup.getField(0).toString()));
								int sums = temp.getField(1).hashCode()+tup.getField(1).hashCode();
								
								temp.setField(1, new IntField(sums));
								
							}
							else {
								Tuple newT = new Tuple(newTd);
								newT.setField(0, tup.getField(0));
								newT.setField(1, tup.getField(1));
								result.add(newT);
								groups.put(newT.getField(0).toString(), result.size() - 1);
							}
						}
					}
					
				}
				System.out.println(result.toString());
				return result;
			}
			else {
				if(this.td.getType(0)==Type.INT) {
					Type[] type = new Type[] {Type.INT};
					String[] fields = new String[] {"Sum"};
					TupleDesc newTd = new TupleDesc(type,fields);
					
					int sums =0;
					if (!tuples.isEmpty()) {
						for (Tuple tup: tuples) {
							sums+= tup.getField(0).hashCode();
						}
					}
					Tuple newT = new Tuple(newTd);
					newT.setField(0, new IntField(sums));
					tuples.clear();
					tuples.add(newT);
				}
			}
			break;
		case AVG:
			if(gb) {
				ArrayList<Tuple> result = new ArrayList<Tuple>();

				//set up Tuple Desc
				Type[] type = new Type[] {this.td.getType(0),this.td.getType(1)};
				String[] fields = new String[] {this.td.getFieldName(0), "Average"};
				TupleDesc newTd = new TupleDesc(type,fields);
				
				//count in each group
				if(this.td.getType(1)==Type.INT) {
					//requires extra map to keep track of the count;
					if(this.td.getType(0)==Type.INT) {
						HashMap<Integer,Integer> groups = new HashMap<Integer,Integer>();
						HashMap<Integer,Integer> counts = new HashMap<Integer,Integer>();
						for(Tuple tup: tuples) {
							if(groups.containsKey(tup.getField(0).hashCode())){
								Tuple temp = result.get(groups.get(tup.getField(0).hashCode()));
								int count = counts.get(tup.getField(0).hashCode())+1;
								int sums = (temp.getField(1).hashCode()+tup.getField(1).hashCode());
								temp.setField(1, new IntField(sums));
								counts.put(tup.getField(0).hashCode(), count);

							}
							else {
								Tuple newT = new Tuple(newTd);
								newT.setField(0, tup.getField(0));
								newT.setField(1, tup.getField(1));
								result.add(newT);
								groups.put(newT.getField(0).hashCode(), result.size() - 1);
								counts.put(newT.getField(0).hashCode(), 1);
							}
						}
						for(Tuple tup: result) {
							int sums = tup.getField(1).hashCode();
							int c = counts.get(tup.getField(0).hashCode());
							tup.setField(1, new IntField(sums/c));
						}
					}
					if(this.td.getType(0)==Type.STRING) {
						HashMap<String,Integer> groups = new HashMap<String,Integer>();
						HashMap<String,Integer> counts = new HashMap<>();
						for(Tuple tup: tuples) {
							if(groups.containsKey(tup.getField(0).toString())){
								Tuple temp = result.get(groups.get(tup.getField(0).toString()));
								int sums = temp.getField(1).hashCode()+tup.getField(1).hashCode();
								int count = counts.get(tup.getField(0).toString())+1;
								
								temp.setField(1, new IntField(sums));
								counts.put(tup.getField(0).toString(), count);
							}
							else {
								Tuple newT = new Tuple(newTd);
								newT.setField(0, tup.getField(0));
								newT.setField(1, tup.getField(1));
								result.add(newT);
								groups.put(newT.getField(0).toString(), result.size() - 1);
								counts.put(newT.getField(0).toString(), 1);
							}
						}
						for(Tuple tup: result) {
							int sums = tup.getField(1).hashCode();
							int c = counts.get(tup.getField(0).toString());
							tup.setField(1, new IntField(sums/c));
						}
					}
					
				}
				return result;
			}
			else {
				if(this.td.getType(0)==Type.INT) {
					Type[] type = new Type[] {Type.INT};
					String[] fields = new String[] {"Average"};
					TupleDesc newTd = new TupleDesc(type,fields);
					
					int avg =0;
					if (!tuples.isEmpty()) {
						for (Tuple tup: tuples) {
							avg+= tup.getField(0).hashCode();
						}
						avg = avg/tuples.size();
					}
					Tuple newT = new Tuple(newTd);
					newT.setField(0, new IntField(avg));
					tuples.clear();
					tuples.add(newT);
				}
			}
			break;
		}
		return tuples;
	}
	
	private ArrayList<Tuple> groupByMinMax(RelationalOperator ro){
		ArrayList<Tuple> result = new ArrayList<Tuple>();
		
		//set up Tuple Desc
		String colName="";
		if(ro.equals(RelationalOperator.GT)) {
			colName = "Max";
		}
		else {
			colName = "Min";
		}
		Type[] type = new Type[] {this.td.getType(0),this.td.getType(1)};
		String[] fields = new String[] {this.td.getFieldName(0), colName};
		TupleDesc newTd = new TupleDesc(type,fields);
		
		//groupby column
		if(this.td.getType(0)==Type.INT) {
			// 1st int: hashcode of group field; 2nd: index in the result
			HashMap<Integer,Integer> groups = new HashMap<Integer,Integer>();
			for(Tuple tup: tuples) {
				//existing group case
				if(groups.containsKey(tup.getField(0).hashCode())) {
					Tuple compare = result.get(groups.get(tup.getField(0).hashCode()));
					if(tup.getField(1).compare(ro, compare.getField(1))) {
						compare.setField(1, tup.getField(1));
					}
				}
				else {
					Tuple newT = new Tuple(newTd);
					newT.setField(0, tup.getField(0));
					newT.setField(1, tup.getField(1));
					result.add(newT);
					groups.put(tup.getField(0).hashCode(), result.size()-1);
				}
			}
		}
		else if(this.td.getType(0)==Type.STRING) {
			HashMap<String,Integer> groups = new HashMap<String,Integer>();
			for(Tuple tup: tuples) {
				//existing group case
				if(groups.containsKey(tup.getField(0).toString())) {
					Tuple compare = result.get(groups.get(tup.getField(0).toString()));
					if(tup.getField(1).compare(ro, compare.getField(1))) {
						compare.setField(1, tup.getField(1));
					}
				}
				else {
					Tuple newT = new Tuple(newTd);
					newT.setField(0, tup.getField(0));
					newT.setField(1, tup.getField(1));
					result.add(newT);
					groups.put(tup.getField(0).toString(), result.size()-1);
				}
			}
		}
		return result;
	}

}
