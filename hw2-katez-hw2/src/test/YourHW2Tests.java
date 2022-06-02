package test;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

import org.junit.Before;
import org.junit.Test;

import hw1.Catalog;
import hw1.Database;
import hw1.HeapFile;
import hw1.IntField;
import hw1.TupleDesc;
import hw2.Query;
import hw2.Relation;

public class YourHW2Tests {

	private HeapFile testhf;
	private TupleDesc testtd;
	private HeapFile ahf;
	private TupleDesc atd;
	private Catalog c;

	@Before
	public void setup() {
		
		try {
			Files.copy(new File("testfiles/test.dat.bak").toPath(), new File("testfiles/test.dat").toPath(), StandardCopyOption.REPLACE_EXISTING);
			Files.copy(new File("testfiles/A.dat.bak").toPath(), new File("testfiles/A.dat").toPath(), StandardCopyOption.REPLACE_EXISTING);
		} catch (IOException e) {
			System.out.println("unable to copy files");
			e.printStackTrace();
		}
		
		c = Database.getCatalog();
		c.loadSchema("testfiles/test.txt");
		
		int tableId = c.getTableId("test");
		testtd = c.getTupleDesc(tableId);
		testhf = c.getDbFile(tableId);
		
		c = Database.getCatalog();
		c.loadSchema("testfiles/A.txt");
		
		tableId = c.getTableId("A");
		atd = c.getTupleDesc(tableId);
		ahf = c.getDbFile(tableId);
	}
	
	@Test
	public void testRenameQ() {
		Query q = new Query("SELECT a1, a2 AS a3 FROM A");
		Relation r = q.execute();

		assertTrue("fail to rename the column", r.getDesc().getFieldName(1).equals("a3"));
	}
	
	@Test
	public void testAggregateMaxQ() {
		Query q = new Query("SELECT MAX(a1) FROM A");
		Relation r = q.execute();
		
		assertTrue("Aggregations should result in one tuple",r.getTuples().size() == 1);
		IntField agg = (IntField) r.getTuples().get(0).getField(0);
		assertTrue("Result of sum aggregation is 530", agg.getValue() ==530);
		
	}

}
