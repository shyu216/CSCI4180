public class BitDemo {
	/* Format the binary string with a space between every 4 digits. Also highlight the string from the first digit = 1. */
	public static String toBinaryString( int i ) {
		String binStr = Integer.toBinaryString( i );
		binStr = "00000000000000000000000000000000".substring( binStr.length() ) + binStr;
		binStr = new StringBuilder( binStr ).reverse().toString();
		binStr = binStr.replaceAll( "....(?!$)", "$0 " );
		binStr = new StringBuilder( binStr ).reverse().toString();
		binStr = binStr.replaceAll( "^([^1]*)1(.*)$", "\u001B[37m$1\u001B[31m1$2\u001B[37m" );
		return binStr;
	}

	/* Print the number in both decimal and binary. */
	public static void printNumber( int num ) {
		System.out.println( "Decimal: \u001B[32m" + num + "\u001B[37m" );
		System.out.println( "Binary : " + toBinaryString( num ) + "\n" );
	}

	public static void main( String[] args ) {
		int tmp;
		int[] numbers = { 4180, 0x79ABCDEF, 0x98ABCDEF };
		System.out.print( "------------------------------------------------\n\n" );
		for ( int num : numbers ) {
			System.out.println( "Original number:" );
			printNumber( num );

			tmp = num << 1;
			System.out.println( "Left shift by 1 bit (<< 1):" );
			printNumber( tmp );

			tmp = num >> 1;
			System.out.println( "Right shift by 1 bit (>> 1):" );
			printNumber( tmp );

			tmp = num >>> 1;
			System.out.println( "Unsigned right shift by 1 bit (>>> 1):" );
			printNumber( tmp );

			tmp = ~ num;
			System.out.println( "Unary bitwise complement (~):" );
			printNumber( tmp );

			System.out.print( "------------------------------------------------\n\n" );
		}
	}
}