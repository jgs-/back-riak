CC=gcc
CFLAGS=-Wall -fPIC -I/usr/include/mozldap -I/usr/include/nspr4

back-riak:
	$(CC) $(CFLAGS) -shared -o back-riak.so back-riak.c
