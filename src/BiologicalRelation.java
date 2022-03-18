
public class BiologicalRelation {
	int degreeOfcousinship; //degree of cousinship
	int degreeOfremoval; //degree of removal
	
	public int getRemoval() {
		return degreeOfremoval;
	}
	public void setRemoval(int removal) {
		this.degreeOfremoval = removal;
	}
	public int getCousin() {
		return degreeOfcousinship;
	}
	public void setCousin(int cousin) {
		this.degreeOfcousinship = cousin;
	}
	public BiologicalRelation(int cousin, int removal) {
		super();
		this.degreeOfcousinship = cousin;
		this.degreeOfremoval = removal;
	}
	
}
