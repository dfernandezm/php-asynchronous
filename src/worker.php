<?php
require("../vendor/autoload.php");

$broker = new Pheanstalk_Pheanstalk("localhost:11300");

function sendResponse($response) {
	global $broker;
	$broker->putInTube("responses", $response);
	echo "Response put in responses queue \n";
	
}

function executeWorker() {
	
	global $broker;
	
	$broker->watchOnly("requests");
		
	while (true) {
		
		echo "Waiting for job...\n";	
		
		while($job = $broker->reserve(0)) {
				
			echo "Processing job with id ".$job->getId()." and payload \n". $job->getData()." \n";
			
			try {			
				echo "Processing...\n";
				sleep(15);
				echo "Processing was successful \n";
				echo "Sending response back...\n";
				sendResponse($job->getId()."-SUCCESS");
				$broker->delete($job);
			} catch (Exception $e) {
				echo "There was an error during the job -- retrying [TODO]";
				$broker->bury($job);
			}
		}
	
		sleep(10);		
	}
}

executeWorker();