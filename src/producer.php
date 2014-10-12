<?php
require("../vendor/autoload.php");

$broker = new Pheanstalk_Pheanstalk("localhost:11300");
$handle = fopen("/tmp/jobs.log", "a");
$childrenFile = "/tmp/numberChildren.txt";
$shmId = shmop_open("100", "c", 0644, 1);

function sendJob($i) {
	
	global $broker;
	global $handle;
	
	fwrite($handle, "Producer: Putting job to requests queue...\n");
	
	$success = false;
	$retryCount = 0;
	$exception = null;
	while(!$success && $retryCount < 5) {
		
		try {
			
			$job = "$i-atyYuiOkWjk1hO\nDOWNLOAD\nNo further actions";
			$jobId = $broker->useTube("requests")->put($job, 60);
			fwrite($handle,"Producer: Job successfully put, id is ".$jobId."\n");
			
			if ($i % 10 == 0) {
				fwrite($handle, "Parent: About to spawn new child to handle jobs, too many... \n");
				checkResponseInForkedExecution();
			}
			
			$success = true;
		} catch (Pheanstalk_Exception $e) {
			fwrite($handle, "Error occurred trying to put job -- retrying\n".$e->getMessage()."\n");
			sleep(1);
			$retryCount++;
			$exception = $e;	
		}
	}
	
	if ($retryCount >= 5) {
		fwrite($handle, "Maximum number of retries exhaustes -- giving up");
		fwrite($handle, "Last exception was \n".$exception->getMessage());
		exit(1);
	}
	
	return $jobId;
}

function checkResponseInForkedExecution() {
	$pid = pcntl_fork();
	global $handle;
	
	if ($pid) {
		fwrite($handle,"Producer: Successfully spawned child with pid $pid for polling responses queue\n");
		modifyNumberOfChildren(true);
		return;
	} else {
		global $broker;
		// Go into daemon 
		$childPid = posix_getpid();
		
		fclose(STDIN);  // Close all of the standard
		fclose(STDOUT); // file descriptors as we
		fclose(STDERR); // are running as a daemon.
		
		$handleChild = fopen("/tmp/jobs.log", "a");
		
		fwrite($handleChild, "Daemon [$childPid]: Checking responses queue...\n");
		
		$start = time();
		$timeout = 5*60; // 5 minutes
		
		$broker->watchOnly("responses");
		
		while(time() - $start < $timeout) {
			fwrite($handleChild, "Daemon [$childPid]: Checking responses... \n");
			$response = readResponse($handleChild, $childPid);
			
			if ($response !== false) {
				fwrite($handleChild, "Daemon [$childPid]: successfully processed responses.\n");
			}
			
			fwrite($handleChild, "Daemon [$childPid] Checking exiting condition");
			
			if (getNumberOfChildren() > 1) {
				fwrite($handleChild, "Daemon [$childPid] Still enough daemons -- exiting");
				modifyNumberOfChildren(false);
				exit(0);
			}
			 
			sleep(5);
			$runningTime = time() - $start;
			fwrite($handleChild, "Daemon [$childPid]: Was $runningTime running... \n");
		}
		
		fwrite($handleChild, "Daemon [$childPid]: Timeout waiting for response \n");
		exit(0);
		
	}
}

function modifyNumberOfChildren($add) {
	global $handle;
	global $shmId;

	$buffer = shmop_read($shmId, 0, 1);
	fwrite($handle, "Read $buffer from children file\n");
	$currentNumber = 0;
	
	if (!$buffer || $buffer == '') {
		// nothing in the file, assuming 0 children
		shmop_write($shmId, "1", 0);
		fwrite($handle, "Writing 1 daemon \n");
	} else {
		 $currentNumber = intval($buffer);
		 $currentNumber = $add ? $currentNumber + 1 : $currentNumber - 1;
		 shmop_write($shmId, $currentNumber, 0);
		 fwrite($handle, "Writing $currentNumber \n");
	}

	return $currentNumber;
}

function getNumberOfChildren() {
	global $handle;
	global $shmId;
	
	$buffer = shmop_read($shmId, 0, 1);
	fwrite($handle, "Read $buffer from children file\n");
	$currentNumber = -1;
	
	if (!$buffer || $buffer == '') {
		$currentNumber = 0;
	} else {
		$currentNumber = intval($buffer);
	}
	
	fwrite($handle, "There are $currentNumber of daemons running\n");
	return $currentNumber;
}

function readResponse($handle, $childPid) {
	
	global $broker;
	
	
	$msg = "Daemon [$childPid]: Push received -- go to check queue for job... \n"; 
	fwrite($handle, $msg);
	$jobs = 0;
	while($job = $broker->reserve(0)) {
		$msg = "Daemon [$childPid]: Processing response back: ".$job->getData()."\n";
		fwrite($handle, $msg);
		// Execute transactionally
		sleep(3);
		$broker->delete($job);
		$jobs++;
	}
	
	if ($jobs > 0) {
		fwrite($handle, "Daemon [$childPid]: Processed $jobs responses.\n");
		return true;
	}
	// no jobs ready
	$msg = "Daemon [$childPid]: No job in the queue keep polling...\n";
	fwrite($handle, $msg);
	return false;
}


for ($i = 0; $i<25; $i++) {
	sendJob($i);
	sleep(1);
}

