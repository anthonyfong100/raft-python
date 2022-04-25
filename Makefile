all:

test:
	./run all

milestone1:
	./run configs/simple-1.json 
	./run configs/simple-2.json 
	./run configs/crash-1.json 

simple:
	./run configs/simple-1.json 

unreliable:
	./run configs/unreliable-1.json 
	./run configs/unreliable-2.json 
	./run configs/unreliable-3.json 

partition:
	./run configs/partition-1.json 
	./run configs/partition-2.json 
	./run configs/partition-3.json 
	./run configs/partition-4.json 

crash:
	./run configs/crash-1.json 
	./run configs/crash-2.json 
	./run configs/crash-3.json 
	./run configs/crash-4.json 

advanced:
	./run configs/advanced-1.json 
	./run configs/advanced-2.json 
	./run configs/advanced-3.json 
	./run configs/advanced-4.json 

unit-test:
	python3 -m unittest discover ./tests/unit -v

integration-test:
	python3 -m unittest discover ./tests/integration -v

ut-integration-test:
	python3 -m unittest discover tests -v
	
clean:
	rm ./raft_python_debug.log
	rm ./simulator_logs.log

view-logs:
	tail -f raft_python_debug.log