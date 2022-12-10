import java.util.HashMap;

public class HashMapDemo {
	public static void main( String[] args ) {
		HashMap<String, String> map = new HashMap<String, String>();
		map.put( "Facebook", "1 Hacker Way" );
		map.put( "Microsoft", "One Microsoft Way" );
		map.put( "Apple", "1 Infinite Loop" );

		for ( String key : map.keySet() ) {
			System.out.println( key + " --> " + map.get( key ) );
		}
	}
}
