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
	python3 -m unittest discover ./tests/unit -v

integration-test:
	python3 -m unittest discover ./tests/integration -v

clean:
	rm ./raft_python_debug.log

view-logs:
	tail -f raft_python_debug.log