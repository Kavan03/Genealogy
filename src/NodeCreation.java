import java.util.ArrayList;
import java.util.List;

// declaring four variables
public class NodeCreation {
	private int parentId;
	private int childId;
	private NodeCreation parent;
	private List<NodeCreation> children;

	// declaring constructor
	public NodeCreation(int parentId, int childId) {
		this.parentId = parentId;
		this.childId = childId;
		this.children = new ArrayList<>();
	}
	// adding children in arraylist if present
	public void addChild(NodeCreation child) {
		if (!this.children.contains(child) && child != null)
			this.children.add(child);
	}
	// declaring getters and setters
	public int getChildId() {
		return childId;
	}

	public void setChildId(int childId) {
		this.childId = childId;
	}

	public int getParentId() {
		return parentId;
	}

	public void setParentId(int parentId) {
		this.parentId = parentId;
	}

	public NodeCreation getParent() {
		return parent;
	}

	public void setParent(NodeCreation parent) {
		this.parent = parent;
	}

	public List<NodeCreation> getChildren() {
		return children;
	}

	public void setChildren(List<NodeCreation> children) {
		this.children = children;
	}
}
