CC=gcc
JSON=/home/ccjim/json
CFLAGS=-Wall -fPIC -I../ -I/usr/include/mozldap -I/usr/include/nspr4 -I$(JSON)/include -L$(JSON)/lib -ljansson

back-riak:
	$(CC) $(CFLAGS) -shared -o back-riak.so back-riak.c

clean:
	@rm *.so
