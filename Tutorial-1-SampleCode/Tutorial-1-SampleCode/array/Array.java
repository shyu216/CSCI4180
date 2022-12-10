public class Array {
	public static void main( String[] args ) {
		char[][] ca = { { 'W', 'A', 'S', 'D' }, { 'I', 'J', 'K', 'L' }, { '1', '2' } };
		for ( int i = 0; i < ca.length; i++ ) {
			System.out.print( "Row #" + i + ":" );
			for ( int j = 0; j < ca[ i ].length; j++ ) {
				System.out.print( "\t" + ca[ i ][ j ] );
			}
			System.out.print( "\n" );
		}
	}
}
