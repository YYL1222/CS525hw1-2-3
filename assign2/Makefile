CC = gcc

all: test_1 test_2

test_1: test_assign2_1.o dberror.o storage_mgr.o buffer_mgr.o buffer_mgr_stat.o
	$(CC) -o $@ $^

test_2: test_assign2_2.o dberror.o storage_mgr.o buffer_mgr.o buffer_mgr_stat.o
	$(CC) -o $@ $^

%.o: %.c dberror.h storage_mgr.h test_helper.h buffer_mgr.h buffer_mgr_stat.h
	$(CC) -c -o $@ $<

clean:
	rm test_1 test_2 *.o