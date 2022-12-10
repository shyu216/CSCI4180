public class Computer {
	CPU intel, amd;
	RAM ram;

	// Static nested class "CPU"
	public static class CPU {
		public int run( int a, int b, char operation ) {
			switch( operation ) {
				case '+': return a + b;
				case '-': return a - b;
				case '*': return a * b;
				case '/': return a / b;
			}
			return 0;
		}
	}

	// Static nested class "RAM"
	public static class RAM {
		public int[] alloc( int size ) {
			if ( size > 0 )
				return new int[ size ];
			return null;
		}
	}

	// Constructor of Computer, which prepares the data structures
	public Computer() {
		this.intel = new CPU();
		this.amd = new CPU();
		this.ram = new RAM();
	}

	public static void main( String[] args ) {
		Computer mac = new Computer();
		int[] memory = mac.ram.alloc( 4 );
		memory[ 0 ] = mac.intel.run( 1, 2, '+' );
		memory[ 1 ] = mac.intel.run( 3, 4, '-' );
		memory[ 2 ] = mac.intel.run( 5, 6, '*' );
		memory[ 3 ] = mac.intel.run( 7, 8, '/' );
		
		for ( int i = 0; i < 4; i++ ) {
			System.out.print( memory[ i ] + " " );
		}
		System.out.println();
	}
}
