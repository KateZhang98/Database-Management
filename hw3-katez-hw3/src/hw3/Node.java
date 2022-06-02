package hw3;

import hw1.Field;

public interface Node {
	
	
	public int getDegree();
	public boolean isLeafNode();
	public boolean isFull();
	public boolean isHalfFull();
	public Field getMaxEntry();
	
	
}
