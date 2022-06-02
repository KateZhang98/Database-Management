package hw3;

import java.util.ArrayList;

import hw1.Field;
import hw1.RelationalOperator;


public class LeafNode implements Node {
	private int degree;
	private ArrayList<Entry> entries;
	
	
	
	public LeafNode(int degree) {
		//your code here
		this.degree = degree;
		this.entries = new ArrayList<Entry>();
	}
	
	public ArrayList<Entry> getEntries() {
		//your code here
		return entries;
	}

	public int getDegree() {
		//your code here
		return this.degree;
	}
	
	public Field getMaxEntry() {
		return entries.get(entries.size()-1).getField();
	}

	public boolean isLeafNode() {
		return true;
	}
	
	public boolean isFull() {
		return entries.size()>degree;
	}
	
	public boolean isHalfFull() {
		return entries.size() > Math.ceil(degree/2.0);
	}

	public void addEntry(Entry e) {
		
		for(Entry en: entries) {
			if(en.getField().compare(RelationalOperator.EQ, e.getField())) {
				return;
			}
			if(en.getField().compare(RelationalOperator.GT, e.getField())) {
				entries.add(entries.indexOf(en), e);
				return;
			}
		}
		entries.add(e);
	}
	
	public void deleteEntry(Entry e) {
		for(Entry en: entries) {
			if(en.getField().compare(RelationalOperator.EQ, e.getField())) {
				entries.remove(en);
				return;
			}
		}
	}
	
	//split & merge method for Bplus tree
	public LeafNode merge(LeafNode n2) {
		LeafNode newNode = new LeafNode(getDegree());
		
		for(Entry e: getEntries()) {
			newNode.addEntry(e);
		}
		for (Entry e: n2.getEntries()) {
			newNode.addEntry(e);
		}
		return newNode;
	}
	
	public LeafNode split(){
		//revise current node & add split node
		
//		LeafNode n1 = new LeafNode(getDegree());
		LeafNode n2 = new LeafNode (getDegree());
		
		ArrayList<Entry> newEntry = new ArrayList<Entry>();
		for(int i = 0;i<getEntries().size();i++) {
			if(i<(int) Math.ceil(getEntries().size()/2.0)) {
				newEntry.add(getEntries().get(i));
			}
			else {
				n2.addEntry(getEntries().get(i));
			}
		}
		
		this.entries = newEntry;
//		System.out.println("left node entries size: "+ this.entries.size());
//		System.out.println("right node entries size: "+ n2.entries.size());
		
		return n2;
	}

}