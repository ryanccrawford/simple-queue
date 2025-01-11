# Simple Queue

**Simple Queue** is a robust **MongoDB-based** job queue, powered by Mongoose change streams. It offers:

- **Automatic job processing** with retries and exponential backoff
- **Dead-letter queue** for jobs that exceed max retries
- **Concurrency control** within a single Node.js process
- **Pause and resume** functionality
- **Customizable logger** (defaults to the `console`)

---

## Table of Contents

1. [Features](#features)
2. [Requirements](#requirements)
3. [Installation](#installation)
4. [Quick Start](#quick-start)
    - [Defining Models](#defining-models)
    - [Creating the Queue](#creating-the-queue)
    - [Starting and Stopping](#starting-and-stopping)
5. [Usage](#usage)
    - [Registering Handlers](#registering-handlers)
    - [Enqueuing Jobs](#enqueuing-jobs)
    - [Pause and Resume](#pause-and-resume)
    - [Concurrency](#concurrency)
6. [Advanced Topics](#advanced-topics)
    - [Dead-Letter Queue](#dead-letter-queue)
    - [Custom Logger](#custom-logger)
    - [Error Handling](#error-handling)
    - [Testing](#testing)
7. [Example Code](#example-code)

---

## Features

- **MongoDB Change Streams**: Automatically reacts to inserted or updated jobs (`status: 'queued'`)
- **Retry Logic**: Configure `maxRetries`, `retryInterval`, `backoffFactor`
- **Dead-Letter Queue**: Jobs exceeding `maxRetries` move to a separate collection
- **Concurrency Control**: Limit how many jobs run in parallel per process
- **Pause/Resume**: Temporarily halt job processing and resume later

---

## Requirements

- **Node.js** (version 14 or later recommended)
- **MongoDB** (replica set or sharded cluster, to enable change streams)
- **Mongoose** (6.x or later)

---

## Installation

Install **Simple Queue** via npm or yarn:

```bash
npm install simple-queue
# or
yarn add simple-queue
````


## Quick Start

#### Defining Models

You need two Mongoose models:

Job Model (the main queue)
```javascript
const mongoose = require('mongoose');

const JobSchema = new mongoose.Schema({
  taskName: { type: String, required: true },
  data: mongoose.Schema.Types.Mixed,
  priority: { type: Number, default: 0 },
  status: {
    type: String,
    enum: ['queued', 'processing', 'completed', 'failed', 'retrying', 'canceled'],
    default: 'queued',
  },
  attempts: { type: Number, default: 0 },
  maxRetries: { type: Number, default: 3 },
  retryInterval: { type: Number, default: 1000 },
  backoffFactor: { type: Number, default: 2 },
  lastError: mongoose.Schema.Types.Mixed,
  nextRunAt: { type: Date, default: null },
  createdAt: { type: Date, default: Date.now },
  updatedAt: { type: Date, default: Date.now },
}, {
  timestamps: true
});

module.exports = mongoose.model('Job', JobSchema);
```

DeadLetterJob Model (for jobs that exceed retries)
```javascript
const mongoose = require('mongoose');

const DeadLetterJobSchema = new mongoose.Schema({
  jobId: { type: mongoose.Schema.Types.ObjectId, required: true, ref: 'Job' },
  originalJob: mongoose.Schema.Types.Mixed,
  error: mongoose.Schema.Types.Mixed,
  reason: { type: String },
  firstOccurred: { type: Date },
  lastOccurred: { type: Date },
  attempts: { type: Number },
  status: { type: String, default: 'pending' },
  component: { type: String },
  createdAt: { type: Date, default: Date.now },
});

module.exports = mongoose.model('DeadLetterJob', DeadLetterJobSchema);

```


### Creating the Queue

```javascript
// queue-setup.js
const mongoose = require('mongoose');
const SimpleQueue = require('simple-queue');
const Job = require('./models/job');
const DeadLetterJob = require('./models/deadLetterJob');

async function connectDB() {
  // Connect to MongoDB (replica set/sharded cluster)
  await mongoose.connect(process.env.MONGO_URI, {
    useNewUrlParser: true,
    useUnifiedTopology: true,
    // ...other options
  });
}

function createQueue() {
  return new SimpleQueue(mongoose.connection, {
    jobModel: Job,
    deadLetterJobModel: DeadLetterJob,
    concurrency: 2, // how many jobs to process in parallel
    logger: console, // or pass a custom logger
  });
}

module.exports = { connectDB, createQueue };

```

### Starting and Stopping

```javascript
(async () => {
    const { connectDB, createQueue } = require('./queue-setup');

    await connectDB();
    const queue = createQueue();

    // Start listening for new or re-queued jobs
    await queue.start();

    // ... do stuff ...

    // To stop listening
    // await queue.stop();
})();

```

# Usage
### Registering Handlers
Define a handler for each task name:
```javascript
queue.process('sendEmail', async (data) => {
  // e.g. data might be { to: 'test@example.com', subject: 'Hello' }
  console.log('Sending email to:', data.to);
  // ... actually send the email ...
});

queue.process('resizeImage', async (data) => {
  // your image-resizing logic here
});

```

### Enqueuing Jobs

```javascript
await queue.enqueue('sendEmail', { to: 'test@example.com' }, {
   priority: 10,
   maxRetries: 5,
   retryInterval: 2000,  // ms
   backoffFactor: 2,
});
```
The job is inserted with status = 'queued' and will be picked up automatically if the queue is running.

### Pause and Resume
```javascript
queue.pause();
// No new jobs will be processed while paused

await queue.resume();
// Queued jobs start processing again

```
### Concurrency
Set a concurrency value in the constructor to limit how many jobs run in parallel
```javascript
const queue = new SimpleQueue(mongoose.connection, {
  jobModel: Job,
  deadLetterJobModel: DeadLetterJob,
  concurrency: 5,
});

```

## Advanced Topics
### Dead-Letter Queue
When a job fails more than maxRetries times, Simple Queue automatically:
- Copies the job to your DeadLetterJob collection
- Removes it from the Job collection
- Logs the error and reason
You can later inspect or re-run these dead-lettered jobs manually if needed.

### Custom Logger
By default, Simple Queue uses console. Provide your own logger if you want different logging behavior:
```javascript
const myLogger = {
  debug: (...args) => { /* custom debug */ },
  info: (...args) => { /* custom info */ },
  warn: (...args) => { /* custom warn */ },
  error: (...args) => { /* custom error */ },
};

const queue = new SimpleQueue(mongoose.connection, {
  jobModel: Job,
  deadLetterJobModel: DeadLetterJob,
  logger: myLogger,
});

```

### Error Handling
- The queue emits 'error' events for critical failures:
```javascript
queue.on('error', (err) => {
  console.error('Queue error:', err);
});

```
- If a job’s handler throws, it triggers the retry logic. Once retries are exceeded, the job moves to the dead-letter queue.

## Example Code
```javascript
// main.js
require('dotenv').config();
const { connectDB, createQueue } = require('./queue-setup');

async function main() {
  await connectDB();
  const queue = createQueue();

  // Register a simple “log” task
  queue.process('logMessage', async (data) => {
    console.log('LOG MESSAGE:', data.message);
  });

  // Start listening
  await queue.start();

  // Enqueue a job
  await queue.enqueue('logMessage', { message: 'Hello from Simple Queue!' });

  // Stop the queue after 5s
  setTimeout(async () => {
    await queue.stop();
    process.exit(0);
  }, 5000);
}

main().catch(console.error);

```