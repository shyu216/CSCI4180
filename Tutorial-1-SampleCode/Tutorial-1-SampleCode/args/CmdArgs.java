public class CmdArgs {
	public static void main( String[] args ) {
		System.out.print( "Number of command-line arguments: " );
		System.out.println( args.length );

		for ( int i = 0; i < args.length; i++ ) {
			System.out.print( "Argument #" + i + ": " );
			System.out.println( args[ i ] );
		}
	}
}
