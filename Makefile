CC=gcc
JSON=/home/ccjim/json/include
CFLAGS=-Wall -fPIC -I../ -I/usr/include/mozldap -I/usr/include/nspr4 -I$(JSON)

back-riak:
	$(CC) $(CFLAGS) -shared -o back-riak.so back-riak.c

clean:
	@rm *.so
