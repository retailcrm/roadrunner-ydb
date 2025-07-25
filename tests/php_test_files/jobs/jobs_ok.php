<?php

use Spiral\RoadRunner\Jobs\Consumer;
use Spiral\RoadRunner\Jobs\Task\ReceivedTaskInterface;

ini_set('display_errors', 'stderr');
require dirname(__DIR__) . "/vendor/autoload.php";

$consumer = new Consumer();

/** @var ReceivedTaskInterface $task */
while ($task = $consumer->waitTask()) {
    try {
        $name = $task->getName();
        $queue = $task->getQueue();
        $driver = $task->getDriver();
        $payload = $task->getPayload();

        $task->ack();
    } catch (\Throwable $e) {
        $task->requeue($e);
    }
}
