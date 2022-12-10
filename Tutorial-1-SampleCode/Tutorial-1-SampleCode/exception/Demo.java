import java.io.IOException;

public class Demo {
	public static void main( String[] args ){
		Processor proc = new Processor();

		try {
			proc.check( args ); // may throw IOException
		} catch ( Exception e ) {
			System.err.println( e );
			System.exit( 1 );
		}

		for ( int i = 0; i < args.length; i++ )
			System.out.println( "Argument #" + i + ": " + args[ i ] );
	}
}
