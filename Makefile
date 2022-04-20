all:

test:
	./run all

milestone1:
	./run configs/simple-1.json 
	./run configs/simple-2.json 
	./run configs/crash-1.json 

simple:
	./run configs/simple-1.json 

unit-test:
	python3 -m unittest -v

clean:
	rm ./raft_python_debug.log