package hw3;


import java.util.ArrayList;
import java.util.prefs.NodeChangeListener;

import hw1.Field;
import hw1.RelationalOperator;


public class BPlusTree {
	
	private int pInner;
	private int pLeaf;
	
	private Node root;
    
    public BPlusTree(int pInner, int pLeaf) {
    	this.pInner = pInner;
    	this.pLeaf = pLeaf;
    	
    	this.root = new LeafNode(this.pLeaf);
    }
    //The keys of an InnerNode should represent the largest value contained in the subtree 
    //to the immediate left.
    public LeafNode search(Field f) {
    	//your code here
    	Node node = root;
    	if(node.isLeafNode()) {
    		for(Entry e:((LeafNode) node).getEntries()) {
    			if(f.compare(RelationalOperator.EQ, e.getField())) {
    				return (LeafNode) node;
    			}
    		}
    		return null;
    	}
    	else {
    		return search(node,f);
    	}
    }
    
    public LeafNode search(Node node, Field f) {
    	if(node.isLeafNode()) {
    		for(Entry e:((LeafNode) node).getEntries()) {
    			if(f.compare(RelationalOperator.EQ, e.getField())) {
    				return (LeafNode) node;
    			}
    		}
    		return null;
    	}
    	else {
    		InnerNode cur = (InnerNode) node;
    		ArrayList<Field> keys = cur.getKeys();
    		if(f.compare(RelationalOperator.LTE, keys.get(0))) {
    			return search(cur.getChildren().get(0),f);
    		}
    		else if(f.compare(RelationalOperator.GT, keys.get(keys.size()-1))) {
    			return search(cur.getChildren().get(cur.getChildren().size()-1),f);
    		}
    		else {
    			for(Field k:keys) {
    				if(k.compare(RelationalOperator.GTE, f)) {
    					int idx = keys.indexOf(k);
    					return search(cur.getChildren().get(idx),f);
    				}
    			}
    		}
    	}
    	return null;
    }
    
    public void insert(Entry e) {
    	//your code here
    	if(root.isLeafNode()) {
    		if(((LeafNode) root).getEntries().size()<this.pLeaf) {
    			((LeafNode) root).addEntry(e);
    		}
    		else {
    			((LeafNode)root).addEntry(e);
    			LeafNode newLeafs = ((LeafNode) root).split();
    			//create inner node
    			InnerNode newIn = new InnerNode(this.pInner);
    			newIn.addChild((LeafNode)root);
    			newIn.addChild(newLeafs);
    			this.root = newIn;
//    			System.out.println(newIn.getKeys());
    		}
    	}
    	else {
    		insertInner(root,e);
    		if(root.isFull()) {		
    			Node rightsub = null;
    			if(root.isLeafNode()) {
    				rightsub = ((LeafNode) root).split();
    			}
    			else {
    				rightsub = ((InnerNode) root).split();
    			}
    			InnerNode newRoot = new InnerNode(this.pInner);
        		newRoot.addChild((InnerNode)root);
        		newRoot.addChild(rightsub);
     
        		root = newRoot;
    		}
    	}
    }
    
    public void insertInner(Node node, Entry e) {
    	if(node.isLeafNode()) {
    		((LeafNode)node).addEntry(e);
    	}
    	else {
    		InnerNode inNode =(InnerNode) node;
    		ArrayList<Field> keys = inNode.getKeys();
    		int index =keys.size();
    		for(Field en: keys) {
    			if(en.compare(RelationalOperator.GT, e.getField())) {
    				index = keys.indexOf(en);
    				break;
    			}
    		}
    		Node child = inNode.getChildren().get(index);
    		insertInner(child,e);
//    		System.out.println(((InnerNode) child).getChildren().size());
    		if(child.isFull()) {
//    			System.out.println("child node is full");
    			insertSplit(child,inNode);
    		}
    		//update keys
    		inNode.update();
    	}
    }
    
    public void insertSplit(Node node, InnerNode parent) {
    	Node right = null;
    	if(node.isLeafNode()) {
			right = ((LeafNode) node).split();
		}
		else {
			right = ((InnerNode) node).split();
		}
		parent.addChild(right);
    }
   
    
    public void delete(Entry e) {
    	//your code here
    	LeafNode cur = search(e.getField());
    	if(cur==null) {
    		System.out.println("cur is null");
    		return;
    	}
    	
    	InnerNode parent = getParent(cur);
//    	System.out.println("Index of cur"+ parent.getChildren().indexOf(cur));
		if(cur.isHalfFull()) {
//			System.out.println("Should be here for deleting 7 & 3! ");
			cur.deleteEntry(e);
		}
		else if(getRoot().isLeafNode()) {
			((LeafNode)getRoot()).deleteEntry(e);
		}
		else {
			//leaf node is less than half full
			//check if siblings is more than half full
			LeafNode lftSib = (LeafNode) parent.getLeft(cur);
			LeafNode rSib = (LeafNode) parent.getRight(cur);
			if(lftSib!=null) {
				//left sibling is more than half full
//				System.out.println(lftSib.getEntries().size()>=Math.ceil(pLeaf/2.0));

				if(lftSib.isHalfFull()) {
					
					Entry borrowEntry = lftSib.getEntries().get(lftSib.getEntries().size()-1);
					lftSib.deleteEntry(borrowEntry);
					cur.deleteEntry(e);
					cur.addEntry(borrowEntry);
					parent.update();
				}
				else {
					//left is less than half full check right
					if(rSib!=null&&rSib.isHalfFull()) {
						Entry borrowEntry = rSib.getEntries().get(0);
						rSib.deleteEntry(borrowEntry);
    					cur.deleteEntry(e);
    					cur.addEntry(borrowEntry);
    					parent.update();
					}
					else {
						//right is also less than half full or right is null
						LeafNode newNode = lftSib.merge(cur);
						newNode.deleteEntry(e);
						parent.deleteChild(lftSib);
						parent.deleteChild(cur);
						parent.addChild(newNode);
						parent.update();
					}
				}
			}
			else {
				//left is null
				if(rSib!=null) {
//    					System.out.println("Should be here when deleting 1");
					if(rSib.isHalfFull()) {
						Entry borrowEntry = rSib.getEntries().get(0);
						rSib.deleteEntry(borrowEntry);
    					cur.deleteEntry(e);
    					cur.addEntry(borrowEntry);
    					parent.update();
					}
					else {
//    						System.out.println("Should be here when deleting 1");
						LeafNode newNode = rSib.merge(cur);
						newNode.deleteEntry(e);
						parent.deleteChild(rSib);
						parent.deleteChild(cur);
						parent.addChild(newNode);
						parent.update();
					}
				}
			}
			
		}
//		System.out.println(parent.getKeys().size());
		//leaf level update complete, check parent level
		if(parent!=null&&!parent.isHalfFull()) {
			InnerNode gPar = getParent(parent);
			//delete level to root case (parent = null)
			if(gPar == parent) {
				this.root = (LeafNode) parent.getChildren().get(0);
			}
			else {
				//push through == right rotation on the parent of the parent
				//grandparents have another child & the other child is more than half full
				InnerNode leftsib = (InnerNode) gPar.getLeft(parent);
				InnerNode rightsib = (InnerNode) gPar.getRight(parent);

				if(leftsib!=null&&(leftsib.getChildren().size()-1)>=Math.ceil(this.pInner/2.0)){
//					System.out.println("Should be here for push thourgh ");
					//last child from the left side 
					Node borrowChild = leftsib.getChildren().get(leftsib.getChildren().size()-1);
					leftsib.deleteChild(borrowChild);
					leftsib.update();
					parent.addChild(borrowChild);
					parent.update();
					gPar.update();
				}
				else if(rightsib!=null&&(rightsib.getChildren().size()-1)>=Math.ceil(this.pInner/2.0)) {
					Node borrowChild = rightsib.getChildren().get(0);
					rightsib.deleteChild(borrowChild);
					rightsib.update();
					parent.addChild(borrowChild);
					parent.update();
					gPar.update();
				}
				else {
					//remove level
					ArrayList<Node> tgt = new ArrayList<Node>();
					if(leftsib!=null) {
						tgt.addAll(leftsib.getChildren());
						tgt.addAll(parent.getChildren());
						gPar.deleteChild(parent);
						gPar.deleteChild(leftsib);
						for(Node n: tgt) {
							gPar.addChild(n);
						}
						gPar.update();
					}
					else if(rightsib!=null) {
						tgt.addAll(parent.getChildren());
						tgt.addAll(rightsib.getChildren());
						gPar.deleteChild(parent);
						gPar.deleteChild(rightsib);
						for(Node n: tgt) {
							gPar.addChild(n);
						}
						gPar.update();
					}
				}
			}
		}
  
    }
    
    public Node getRoot() {
    	//your code here
    	if(this.root.isLeafNode()&&((LeafNode) this.root).getEntries().size()==0) {
    		return null;
    	}
    	return this.root;
    }
    
    public InnerNode getParent(Node n) {
    	if(getRoot().isLeafNode()) {
			return null;
		}
    	InnerNode cur = (InnerNode) getRoot();
    	if(n.isLeafNode()) {
    		//going down the tree to find the node that has n as the leafnode
    		while(!cur.getChildren().get(0).isLeafNode()) {
    			int idx = cur.getKeys().size();
    			for(Field k: cur.getKeys()) {
    				if(k.compare(RelationalOperator.GTE, ((LeafNode)n).getMaxEntry())) {
    					idx = cur.getKeys().indexOf(k);
    					break;
    				}
    			}
    			cur = (InnerNode) cur.getChildren().get(idx);
    		}
    		return cur;
    	}
    	else {
    		ArrayList<Field> keys = ((InnerNode) n).getKeys();
    		Field minKey = keys.get(0);
    		int idx = cur.getKeys().size();
    		for(Field k: cur.getKeys()) {
    			if(k.compare(RelationalOperator.GTE, minKey)) {
    				idx = cur.getKeys().indexOf(k);
    				break;
    			}
    		}
    		while(!cur.getChildren().get(idx).isLeafNode()&&((InnerNode) cur.getChildren().get(idx)).getKeys().get(0)!=minKey) {
    			cur = (InnerNode) cur.getChildren().get(idx);
    			idx = cur.getKeys().size();
    			for(Field k: cur.getKeys()) {
    				if(k.compare(RelationalOperator.GTE, minKey)) {
    					idx = cur.getKeys().indexOf(k);
    					break;
    				}
    			}
    		}
    		return cur;
    	}
    }

}
