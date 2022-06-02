package hw2;

import java.util.ArrayList;
import java.util.List;

import hw1.Catalog;
import hw1.Database;
import hw1.Tuple;
import hw1.TupleDesc;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.util.TablesNamesFinder;

public class Query {

	private String q;
	
	public Query(String q) {
		this.q = q;
	}
	
	public Relation execute()  {
		Statement statement = null;
		try {
			//statement is the root of parse tree.
			statement = CCJSqlParserUtil.parse(q);
		} catch (JSQLParserException e) {
			System.out.println("Unable to parse query");
			e.printStackTrace();
		}
		// since we are only doing "select", we can cast "select" in the front. 
		Select selectStatement = (Select) statement;
		PlainSelect sb = (PlainSelect)selectStatement.getSelectBody();
		
		//your code here
		Catalog c = Database.getCatalog();
		
		
		//first table
//		int tableId =c.getTableId(tables.get(0).getName());
//		ArrayList<Tuple> ogTuples = c.getDbFile(tableId).getAllTuples();
//		TupleDesc ogTd = c.getTupleDesc(tableId);
//		Relation ogTable = new Relation(ogTuples,ogTd);
		
		// access the nodes in sb, and call the relational operations to execute the nodes. 
		//pay attention to the order
		//In particular, you will probably want to use the following classes from the parse tree: 
		//Select, PlainSelect, Table, Join, Expression, EqualsTo, SelectItem.
		
		//access FROM clause (tables) (getFromItem)
		TablesNamesFinder tablenames = new TablesNamesFinder();
		List<String> tnames = tablenames.getTableList(statement);
		System.out.println(tnames.toString());
		int tableId =c.getTableId(tnames.get(0));
		ArrayList<Tuple> ogTuples = c.getDbFile(tableId).getAllTuples();
//		System.out.println("orginal table: "+ ogTuples.toString());
		TupleDesc ogTd = c.getTupleDesc(tableId);
//		System.out.println("Table Desc: "+ ogTd.toString());

		Relation ogTable = new Relation(ogTuples,ogTd);
	
		//process JOINs if there are  any (getJoins) returns a list
		List<Join> joinList = sb.getJoins();
		Relation joins = ogTable;
		
		//for each join
		if(joinList!=null) {
//			System.out.println("There is join in the query");
			for(Join j: joinList) {
			
				FromItem rightItem = (Table) j.getRightItem();
//				System.out.println("og Table columns: "+ joins.getDesc().toString());
		
//				System.out.println("right table name"+rightItem.toString());
				ArrayList<Tuple> rightTuples = c.getDbFile(c.getTableId(rightItem.toString())).getAllTuples();
				TupleDesc rightTd = c.getDbFile(c.getTableId(rightItem.toString())).getTupleDesc();
				Relation right = new Relation(rightTuples,rightTd);
//				System.out.println("right tuple desc: "+ rightTd.toString());
				
				//get on expression (cast to equals?)
				String[] onexp = j.getOnExpression().toString().split("=");
				
//				String leftTable = onexp[0].split("\\.")[0].replaceAll("\\s", "");
				String rightTable = onexp[1].split("\\.")[0].replaceAll("\\s", "");
				
				String leftcol = onexp[0].substring(onexp[0].lastIndexOf(".")+1).replaceAll("\\s", "");
				String rightcol = onexp[1].substring(onexp[1].lastIndexOf(".")+1).replaceAll("\\s", "");
				
				if(!rightItem.toString().toLowerCase().equals(rightTable.toLowerCase())) {
					String x = rightcol;
					rightcol =leftcol;
					leftcol = x;
				}
				
				int fieldleft = joins.getDesc().nameToId(leftcol);
				int fieldright = right.getDesc().nameToId(rightcol);
				
//				System.out.println("left col int: "+ fieldleft);
//				System.out.println("right col name: "+ fieldright);
				
				
//				Relation joins = joins.join(right, tableId, tableId)
				joins = joins.join(right, fieldleft, fieldright);
//				System.out.println("tuples in the joined table: "+ joins.getTuples().toString());

				
			}
		}
//		System.out.println("columns until this point" + joins.getDesc().toString());
		//WHERE: select operation;filtering all the tuples satisfying the WHERE Condition
		Relation where=joins;
		//get the where condition:
		WhereExpressionVisitor wherevisitor = new WhereExpressionVisitor();
		if(sb.getWhere()!=null) {
//			System.out.println("There is WHERE condition in the query");
			sb.getWhere().accept(wherevisitor);
			String col = wherevisitor.getLeft();
			
			int wherecol = where.getDesc().nameToId(col);

			where = where.select(wherecol, wherevisitor.getOp(), wherevisitor.getRight());
		}
		
		
		//SELECT (GROUP BY / aggregates)
		Relation selected = where;
		
		List<SelectItem> selectlist = sb.getSelectItems();
		ArrayList<Integer> selectcol = new ArrayList<Integer>();
		ArrayList<Integer> renameColNum = new ArrayList<Integer>();
		ArrayList<String> renameColStr = new ArrayList<String>();
	
		ColumnVisitor colvisitor = new ColumnVisitor();
//		System.out.println("columns until this point"+ selected.getDesc().toString());
		for(SelectItem col: selectlist) {
			
			col.accept(colvisitor);
//			System.out.println("if there is aggregation: "+ colvisitor.isAggregate());
			String column = colvisitor.isAggregate()? colvisitor.getColumn():col.toString();
			
//			System.out.println("column name" + column);
			System.out.println("column has as " + column.toLowerCase().contains("as"));

			int colfield = 0;

//			System.out.println("select column " + column);
			if(column.equals("*")) {
				if(colvisitor.isAggregate()) {
					colfield = 0;
				}
				for (int i = 0; i < where.getDesc().numFields(); i++) {
					selectcol.add(i);
				}
				break;
			}
			else {
//				System.out.println("select column: "+ column);
				if(column.toLowerCase().contains("as")) {
					String[] temp = column.toLowerCase().split("as");
					
					column = temp[0].replaceAll("\\s", "");
					
					colfield = selected.getDesc().nameToId(column);
					
					renameColNum.add(colfield);
					renameColStr.add(temp[1].replaceAll("\\s", ""));
//					System.out.println("before replace column name"+ temp[1].replaceAll("\\s", "").chars().count());
					
				}
				else {
					colfield = selected.getDesc().nameToId(column);
				}
//				
//				System.out.println("There is an AS in function");
//				System.out.println("rename name: "+ name.getName());
				
			}
			
			if(!selectcol.contains((Integer) colfield)) {
				selectcol.add((Integer) colfield);
			}

			
//			
			
		}
//		System.out.println("select list: " + selectcol.size());
		selected = selected.project(selectcol);
//		System.out.println("Size of tuples in the table after select" + selected.getTuples().toString());
		//aggregates and groupby
		Relation agg = selected;
		boolean groupby = sb.getGroupByColumnReferences()!=null;
//		System.out.println(groupby);

		if(colvisitor.isAggregate()) {
			agg = agg.aggregate(colvisitor.getOp(), groupby);
		}
				
		//AS operation
		if(renameColNum.size()!=0) {
			try {
				agg = agg.rename(renameColNum, renameColStr);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	
		return agg;
		
	}
}
