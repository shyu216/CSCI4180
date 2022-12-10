public class CloneDemo {
	public static void main( String[] args ) {
		Elephant e1 = new Elephant( 5 );
		Elephant e2;

		e2 = e1; // Copy e1 to e2
		e2.setAge( 3 );

		System.out.println( "e1's age: " + e1.getAge() );
		System.out.println( "e2's age: " + e2.getAge() );
	}
}
