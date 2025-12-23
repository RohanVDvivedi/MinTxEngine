gcc -Wall ./test_serde_log_record.c -o testlr.out -lmintxengine -ltuplestore -lwale -lz -lbufferpool -lblockio -lboompar -llockking -lcutlery -lpthread

gcc -Wall ./test_mindb.c -o test_mindb.out -I. -lmintxengine -ltupleindexer -ltuplestore -lwale -lz -lbufferpool -lblockio -lboompar -llockking -lcutlery -lpthread

gcc -Wall ./test_pah.c -o test_pah.out -I. -lmintxengine -lbufferpool -lblockio -lboompar -llockking -lcutlery -lpthread