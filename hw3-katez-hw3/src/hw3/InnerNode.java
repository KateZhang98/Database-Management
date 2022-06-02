package hw3;

import java.util.ArrayList;

import hw1.Field;
import hw1.RelationalOperator;

public class InnerNode implements Node {
	
	private int degree;
	private ArrayList<Node> children;
	private ArrayList<Field> keys; 
	
	public InnerNode(int degree) {
		this.degree = degree;
		this.children = new ArrayList<Node>();
		this.keys = new ArrayList<Field>();
		//your code here
	}
	
	public ArrayList<Field> getKeys() {
		//your code here
		return keys;
	}
	
	
	public ArrayList<Node> getChildren() {
		//your code here
		return children;
	}

	public int getDegree() {
		//your code here
		return degree;
	}
	
	public boolean isLeafNode() {
		return false;
	}
	public Field getSearchKey(Field k) { 
		int index = getKeys().indexOf(k);
		return children.get(index).getMaxEntry();
	}
	
	public Field getMaxEntry() {
		return children.get(children.size()-1).getMaxEntry();
	}
	// is full method
	public boolean isFull() {
		return keys.size()>degree-1;
	}
	
	public boolean isHalfFull() {
		return children.size() >= Math.ceil(degree/2.0);
	}
	
	public void update() {
		ArrayList<Field> newk = new ArrayList<Field>();
		if(children.size()==1) {
			newk.add(children.get(0).getMaxEntry());
		}
		for(int i=0;i<children.size()-1;i++) {
			newk.add(children.get(i).getMaxEntry());
//			System.out.println(children.get(i).getMaxEntry());
		}
		this.keys = newk;
	}
	
	//add a child to the node
	public void addChild(Node node) {
		Field key = node.getMaxEntry();
		
		for(Node child: children) {
			if(child.getMaxEntry().compare(RelationalOperator.GT, key)) {
				children.add(children.indexOf(child),node);
//				System.out.println("index of newe node in the children: "+ children.indexOf(node));
				update();
				return;
			}
		}
		
		children.add(node);
		update();
	}
	
	public void deleteChild(Node n) {
		children.remove(n);
		update();
	}

	public Node getLeft(Node n) {
		if(children.indexOf(n)==0) {
			return null;
		}
		else {
			return children.get(children.indexOf(n)-1);
		}
	}
	
	public Node getRight(Node n ) {
		if(children.indexOf(n)==children.size()-1) {
			return null;
		}
		else {
			return children.get(children.indexOf(n)+1);
		}
	}
	//split & merge method for Bplus tree
	public InnerNode merge(InnerNode n2){
		InnerNode node = new InnerNode(this.degree);
		
		for (Node n: getChildren()) {
			node.addChild(n);
		}
		for (Node n:n2.getChildren()) {
			node.addChild(n);
		}
		return node;
	}
	
	public InnerNode split(){
		InnerNode n2 = new InnerNode (getDegree());
		
		ArrayList<Node> newChildren = new ArrayList<Node>();
		
		for(int i = 0;i<getChildren().size();i++) {
			if(i<(int) Math.ceil(getChildren().size()/2.0)) {
				newChildren.add(getChildren().get(i));
			}
			else {
				n2.addChild(getChildren().get(i));
			}
		}

		this.children = newChildren;
		this.update();
		Field k = this.getMaxEntry();
		this.keys.remove(k);
		n2.update();
		return n2;
	}
	
	public void addKey(Field k) {
		
	}

}