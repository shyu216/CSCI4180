public class Matrix {
	public static void main( String[] args ) {
		int[][] matrix = new int[ 3 ][ 3 ];
		for( int i = 0; i < matrix.length; i++ ) {
			for( int j = 0; j < matrix[ i ].length; j++ ) {
				matrix[ i ][ j ] = i * 3 + j;
			}
		}

		System.out.println( "Original matrix:" );
		for( int i = 0; i < matrix.length; i++ ) {
			for( int j = 0; j < matrix[ i ].length; j++ ) {
				System.out.print( matrix[ i ][ j ] + "\t" );
			}
			System.out.println();
		}

		matrix[ 0 ] = new int[ 5 ];
		matrix[ 2 ] = new int[ 2 ];
		for( int k = 0; k < 5; k++ ) {
			matrix[ 0 ][ k ] = 10 + k;
		}
		matrix[ 2 ][ 0 ] = 20;
		matrix[ 2 ][ 1 ] = 21;

		System.out.println( "\nModified matrix:" );
		for( int i = 0; i < matrix.length; i++ ) {
			for( int j = 0; j < matrix[ i ].length; j++ ) {
				System.out.print( matrix[ i ][ j ] + "\t" );
			}
			System.out.println();
		}
	}
}
