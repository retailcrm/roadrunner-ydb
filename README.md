# RoadRunner YDB Plugin

A plugin for [RoadRunner](https://roadrunner.dev/) that provides integration with [YDB (Yandex Database)](https://ydb.tech/) for job queue functionality.

## Overview

This plugin allows PHP applications using RoadRunner to use YDB as a backend for job queues. It implements the RoadRunner jobs API and provides a driver for YDB's topic service, enabling message-based job processing with YDB.

## Features

- Push jobs to YDB topics
- Consume jobs from YDB topics
- Pause and resume job processing
- Configure job priorities
- Secure connections with TLS
- Authentication with static credentials

## Requirements

- Go 1.24 or higher
- RoadRunner v2024.2.0 or higher
- YDB instance

## Configuration

Add the following to your `.rr.yaml` configuration file:

```yaml
ydb:
  endpoint: "grpcs://ydb.serverless.yandexcloud.net:2135"
  static_credentials:
    user: "your-user"
    password: "your-password"
  tls:
    ca: "/path/to/ca.crt"

jobs:
  pipelines:
    example:
      driver: ydb
      topic: "your-topic"
      priority: 10
      producer_options:
        id: "producer-id"
      consumer_options:
        name: "consumer-name"
```

### Configuration Options

#### Global Options

| Option | Description | Default |
|--------|-------------|---------|
| `endpoint` | YDB endpoint URL | Required |
| `static_credentials.user` | Username for authentication | Optional |
| `static_credentials.password` | Password for authentication | Optional |
| `tls.ca` | Path to CA certificate for TLS | Optional |

#### Pipeline Options

| Option | Description | Default |
|--------|-------------|---------|
| `topic` | YDB topic name | Required |
| `priority` | Job priority | 10 |
| `producer_options.id` | Producer ID | Generated |
| `consumer_options.name` | Consumer name | Generated |

## Usage

### Pushing Jobs

```php
use Spiral\RoadRunner\Jobs\Jobs;

$jobs = new Jobs();
$queue = $jobs->connect('example');

$queue->push(
    'job-name',
    ['data' => 'value'],
    [
        'topic' => 'your-topic',
        'priority' => 1,
    ]
);
```

### Processing Jobs

```php
use Spiral\RoadRunner\Jobs\Consumer;
use Spiral\RoadRunner\Jobs\Jobs;

$jobs = new Jobs();
$consumer = new Consumer();

while ($job = $consumer->waitJob()) {
    try {
        // Process job
        $payload = $job->getPayload();
        // Your job handling logic here
        
        $job->complete();
    } catch (\Throwable $e) {
        $job->fail($e);
    }
}
```

## Testing

The plugin includes integration tests that verify its functionality with YDB. To run the tests:

1. Make sure you have a YDB instance available for testing
2. Configure the test environment in `tests/configs/.rr-init.yaml`
3. Run the tests:

```bash
go test -v ./tests
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Links

- [RoadRunner Documentation](https://roadrunner.dev/)
- [YDB Documentation](https://ydb.tech/docs)
- [RoadRunner Jobs Documentation](https://docs.roadrunner.dev/queues-and-jobs/overview-queues)
