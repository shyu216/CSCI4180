import java.util.Scanner;

public class ConsoleInputDemo {
	public static void main( String[] args ) {
		Scanner scanner = new Scanner( System.in );
		String line;
		int i = 0, j;
		double k;

		System.out.print( "> " );
		while( scanner.hasNextLine() ) {
			i++;
			line = scanner.nextLine();
			if ( line.equals( "end" ) )
				break;
			System.out.printf( "Line #%-4d: %s\n", i, line );
			System.out.print( "> " );
		}
		System.err.println( "\u001B[31mLeaving double mode and entering integer mode...\u001B[37m" );

		i = 0;
		System.out.print( ">> " );
		while( scanner.hasNextInt() ) {
			i++;
			j = scanner.nextInt();
			System.out.printf( "Integer #%-4d: %d\n>> ", i, j );
		}
		scanner.nextLine();
		System.err.println( "\u001B[31mInvalid input: Leaving integer mode and entering double mode...\u001B[37m" );

		i = 0;
		System.out.print( ">>> " );
		while( scanner.hasNextDouble() ) {
			i++;
			k = scanner.nextDouble();
			System.out.printf( "Double #%-4d: %f\n>> ", i, k );
		}
		System.err.println( "\u001B[31mInvalid input: Leaving double mode and bye!\u001B[37m" );
	}
}