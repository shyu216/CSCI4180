public class Elephant {
	int age;
	public Elephant( int age ) {
		this.age = age;
	}

	public void setAge( int age ) {
		this.age = age;
	}

	public int getAge() {
		return this.age;
	}

	public Elephant clone() {
		Elephant copy = new Elephant( this.age );
		return copy;
	}
}
