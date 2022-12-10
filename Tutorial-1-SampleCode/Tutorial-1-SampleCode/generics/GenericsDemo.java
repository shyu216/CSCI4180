public class GenericsDemo {
	public static void main( String[] args ) {
		Data<String> s = new Data<String>( "String" );
		Data<Integer> i = new Data<Integer>( new Integer( 4180 ) );
		System.out.println( "Data<String> s : " + s.getData() );
		System.out.println( "Data<Integer> i : " + i.getData() );
	}
}
