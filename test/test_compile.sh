#gcc ./test_serde_log_record.c -o testlr.out -lmintxengine -ltuplestore -lwale -lz -lbufferpool -lblockio -lboompar -lrwlock -lserint -lcutlery -lpthread

gcc ./test_bplus_tree.c -o testbpt.out -I. -lmintxengine -ltupleindexer -ltuplestore -lwale -lz -lbufferpool -lblockio -lboompar -lrwlock -lserint -lcutlery -lpthread