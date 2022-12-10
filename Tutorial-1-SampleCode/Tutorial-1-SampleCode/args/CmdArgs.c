#include <stdio.h>

int main( int argc, char **argv ) {
	int i;
	printf( "Number of command-line arguments: " );
	printf( "%d\n", argc );

	for ( i = 0; i < argc; i++ ) {
		printf( "Argument #%d: ", i );
		printf( "%s\n", argv[ i ] );
	}

	return 0;
}
