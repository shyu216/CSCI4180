public class Main {
	public static String getResultStr( boolean b ) {
		if ( b ) {
			return "\u001B[32m" + "true" + "\u001B[37m";
		} else {
			return "\u001B[31m" + "false" + "\u001B[37m";
		}
	}

	public static void main( String[] args ) {
		SuperClass superObj = new SuperClass();
		SubClass subObj = new SubClass();
		SubSubClass subSubObj = new SubSubClass();

		System.out.println( "superObj  : " + superObj.whoami() );
		System.out.println( "subObj    : " + subObj.whoami() );
		System.out.println( "subSubObj : " + subSubObj.whoami() );
		
		System.out.print( "\n" );

		// superObj //
		System.out.println( "superObj instanceof SuperClass ? " + getResultStr( superObj instanceof SuperClass ) );
		System.out.println( "superObj instanceof SubClass ? " + getResultStr( superObj instanceof SubClass ) );
		System.out.println( "superObj instanceof SubSubClass ? " + getResultStr( superObj instanceof SubSubClass ) );
		System.out.print( "\n" );

		// subObj //
		System.out.println( "subObj instanceof SuperClass ? " + getResultStr( subObj instanceof SuperClass ) );
		System.out.println( "subObj instanceof SubClass ? " + getResultStr( subObj instanceof SubClass ) );
		System.out.println( "subObj instanceof SubSubClass ? " + getResultStr( subObj instanceof SubSubClass ) );
		System.out.print( "\n" );

		// subSubObj //
		System.out.println( "subSubObj instanceof SuperClass ? " + getResultStr( subSubObj instanceof SuperClass ) );
		System.out.println( "subSubObj instanceof SubClass ? " + getResultStr( subSubObj instanceof SubClass ) );
		System.out.println( "subSubObj instanceof SubSubClass ? " + getResultStr( subSubObj instanceof SubSubClass ) );
	}
}