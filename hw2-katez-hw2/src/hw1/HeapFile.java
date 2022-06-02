package hw1;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * A heap file stores a collection of tuples. It is also responsible for managing pages.
 * It needs to be able to manage page creation as well as correctly manipulating pages
 * when tuples are added or deleted.
 * @author Sam Madden modified by Doug Shook
 *
 */
public class HeapFile {
	
	public static final int PAGE_SIZE = 4096;
	private File file;
	private TupleDesc type;
	
	/**
	 * Creates a new heap file in the given location that can accept tuples of the given type
	 * @param f location of the heap file
	 * @param types type of tuples contained in the file
	 */
	public HeapFile(File f, TupleDesc type) {
		//your code here
		this.file=f;
		this.type = type;
	}
	
	public File getFile() {
		//your code here
		return this.file;
	}
	
	public TupleDesc getTupleDesc() {
		//your code here
		return this.type;
	}
	
	/**
	 * Creates a HeapPage object representing the page at the given page number.
	 * Because it will be necessary to arbitrarily move around the file, a RandomAccessFile object
	 * should be used here.
	 * @param id the page number to be retrieved
	 * @return a HeapPage at the given page number
	 */
	public HeapPage readPage(int id) {
		//your code here
		byte[] temp = new byte[PAGE_SIZE];
		try {
			RandomAccessFile page = new RandomAccessFile(file,"r");
			page.seek(id*PAGE_SIZE);
			page.read(temp);
			page.close();
			HeapPage hp = new HeapPage(id, temp,this.getId());
			return hp;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * Returns a unique id number for this heap file. Consider using
	 * the hash of the File itself.
	 * @return
	 */
	public int getId() {
		//your code here
		return this.hashCode();
	}
	
	/**
	 * Writes the given HeapPage to disk. Because of the need to seek through the file,
	 * a RandomAccessFile object should be used in this method.
	 * @param p the page to write to disk
	 * @throws IOException 
	 */
	public void writePage(HeapPage p) throws IOException {
		//your code here
		try {
			RandomAccessFile tempFile = new RandomAccessFile(file,"rw");
			tempFile.seek(PAGE_SIZE*p.getId());
			tempFile.write(p.getPageData());
			tempFile.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}
	
	/**
	 * Adds a tuple. This method must first find a page with an open slot, creating a new page
	 * if all others are full. It then passes the tuple to this page to be stored. It then writes
	 * the page to disk (see writePage)
	 * @param t The tuple to be stored
	 * @return The HeapPage that contains the tuple

	 */
	public HeapPage addTuple(Tuple t) throws Exception {
		//your code here
		for(int i = 0;i<getNumPages();i++) {
			HeapPage hp = readPage(i);
			for(int j=0;j<hp.getNumSlots();j++) {
				if(hp.slotOccupied(j)==false) {
					try {
						hp.addTuple(t);
						writePage(hp);
						return hp;
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}
		HeapPage hp = new HeapPage(getNumPages(),new byte[PAGE_SIZE], this.getId());
		hp.addTuple(t);
		this.writePage(hp);
		return hp;
//		return null;
 
	}
	
	/**
	 * This method will examine the tuple to find out where it is stored, then delete it
	 * from the proper HeapPage. It then writes the modified page to disk.
	 * @param t the Tuple to be deleted
	 */
	public void deleteTuple(Tuple t){
		//your code here
		for(int i=0;i<getNumPages();i++) {
			HeapPage hp = readPage(i);
			try {
				hp.deleteTuple(t);
				byte[] newPage = hp.getPageData();
				RandomAccessFile temp = new RandomAccessFile(file,"rw");
				temp.seek(PAGE_SIZE*i);
				temp.write(newPage);
				temp.close();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Returns an ArrayList containing all of the tuples in this HeapFile. It must
	 * access each HeapPage to do this (see iterator() in HeapPage)
	 * @return
	 */
	public ArrayList<Tuple> getAllTuples() {
		//your code here
		ArrayList<Tuple> allTuples = new ArrayList<Tuple>();
		for(int i =0;i<getNumPages();i++) {
			HeapPage hpTemp = readPage(i);
			Iterator<Tuple> iterator = hpTemp.iterator();
			while (iterator.hasNext()) {
				allTuples.add(iterator.next());
			}
		}
//		System.out.println("hf get all tuples"+allTuples);
		return allTuples;
	}
	
	/**
	 * Computes and returns the total number of pages contained in this HeapFile
	 * @return the number of pages
	 */
	public int getNumPages() {
		//your code here
		return (int) Math.ceil(file.length()/PAGE_SIZE);
	}
}
