import java.io.IOException;

public class Processor {
	public void check( String[] args ) throws IOException {
		if ( args.length == 0 ) {
			// Raise exception
			throw new IOException( "Insufficient arguments." );
		}
	}
}
