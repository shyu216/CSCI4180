public class ConsoleOutputDemo {
	public static void main( String[] args ) {
		double d;

		System.out.print( "Standard output stream       : " );
		System.out.println( "Print something with a new line at the end." );

		System.err.print( "Standard error output stream : " );
		System.err.println( "We can also use standard error output stream to print error messages." );

		d = Math.random();
		System.out.print( "\nYou can print the number directly with print* functions." );
		System.out.println( d );

		d = Math.random();
		System.err.print( "We also have C-styled printf() in Java: " );
		System.err.printf( "%+015.9f\n", d );
	}
}
