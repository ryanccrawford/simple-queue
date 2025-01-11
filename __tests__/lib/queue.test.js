/**
 * @file queue.test.js
 * Jest tests for the "best in class" Queue library, relying on real replica set (MongoMemoryReplSet).
 *
 * We use dynamic imports in the queue code (import('serialize-error')).
 * This test file remains mostly the same, but we rely on having set up transformIgnorePatterns
 * (and possibly a transform) in jest.config.js so that dynamic import of ESM from `node_modules`
 * is allowed.
 */

const mongoose = require('mongoose');
const Queue = require('../../lib/queue');  // Adjust path to your library
const Job = require('../../lib/models/job');  // Adjust path to your Job model
const DeadLetterJob = require('../../lib/models/deadLetterJob');  // Adjust path
const { connectDB, disconnectDB, clearDB } = require('../setup/test-setup');

/**
 * Helper to delay a certain # of ms
 */
function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Optional custom logger for capturing logs
 */
const testLogger = {
    debug: jest.fn(console.debug),
    info: jest.fn(console.info),
    warn: jest.fn(console.warn),
    error: jest.fn(console.error),
};

describe('Queue Integration Tests (with dynamic import ESM in node_modules)', () => {
    let queue;

    beforeAll(async () => {
        // Connect to the in-memory replica set
        await connectDB();

        // Create a queue instance with concurrency=2
        queue = new Queue(mongoose.connection, {
            jobModel: Job,
            deadLetterJobModel: DeadLetterJob,
            concurrency: 2,
            logger: testLogger,
        });

        // Register a success + fail tasks
        queue.process('successTask', async () => {
            // Succeed
        });
        queue.process('failTask', async () => {
            throw new Error('Simulated failTask error');
        });

        // Start the queue (opens the change stream)
        await queue.start();
    });

    afterAll(async () => {
        // Stop the queue and disconnect
        await queue.stop();
        await disconnectDB();
    });

    beforeEach(async () => {
        await clearDB();
        jest.clearAllMocks();
    });

    // --------------------------------------------------------------------------
    // Basic test: auto-process a success job
    // --------------------------------------------------------------------------
    test('should automatically process a successful job and mark it as completed', async () => {
        const job = await queue.enqueue('successTask', { foo: 'bar' });

        // Wait for processing
        await sleep(300);

        const updated = await Job.findById(job._id);
        expect(updated.status).toBe('completed');
    });

    // --------------------------------------------------------------------------
    // Failing job => retry => then dead letter
    // --------------------------------------------------------------------------
    test('should fail a job until maxRetries, then move to DLQ (skip retry checks)', async () => {
        // Pause the queue so we can confirm initial "queued" status
        queue.pause();

        // Enqueue a failing job with short intervals
        const job = await queue.enqueue(
            'failTask',
            {},
            { maxRetries: 2, retryInterval: 100, backoffFactor: 2 }
        );

        // Confirm the job is initially queued
        let doc = await Job.findById(job._id);
        expect(doc).toBeTruthy();
        expect(doc.status).toBe('queued');

        // Resume so it will start processing
        queue.resume();

        // Wait enough time for all retries + final DLQ
        // (2 maxRetries = 3 attempts total => final attempt should push it to DLQ)
        await new Promise((r) => setTimeout(r, 1500));

        // The job should be removed from the main collection
        doc = await Job.findById(job._id);
        expect(doc).toBeNull();

        // Check that it’s in the dead-letter queue
        const dlqDoc = await DeadLetterJob.findOne({ jobId: job._id });
        expect(dlqDoc).toBeDefined();
        expect(dlqDoc.reason).toMatch(/max retries/i);
        expect(dlqDoc.attempts).toBe(2);
    });

    // --------------------------------------------------------------------------
    // Cancel a job
    // --------------------------------------------------------------------------
    test('should cancel a queued job before it processes', async () => {
        queue.pause();
        const job = await queue.enqueue('successTask', { foo: 'bar' });

        // Still queued
        let doc = await Job.findById(job._id);
        expect(doc.status).toBe('queued');

        // Cancel
        await queue.cancelJob(job._id);

        doc = await Job.findById(job._id);
        expect(doc.status).toBe('canceled');

        // Resume if you want (not strictly needed for this test)
        queue.resume();
    });

    // --------------------------------------------------------------------------
    // Pause & Resume
    // --------------------------------------------------------------------------
    test('should pause the queue and prevent processing, then resume', async () => {
        // A slow handler
        queue.process('slowTask', async () => {
            await sleep(500);
        });

        queue.pause();
        const job = await queue.enqueue('slowTask', { foo: 'bar' });

        await sleep(300);
        let doc = await Job.findById(job._id);
        expect(doc.status).toBe('queued');

        queue.resume();
        await sleep(700);

        doc = await Job.findById(job._id);
        expect(doc.status).toBe('completed');
    });

    // --------------------------------------------------------------------------
    // Concurrency=2
    // --------------------------------------------------------------------------
    test('should respect concurrency=2 by processing jobs in parallel', async () => {
        const slowHandler = jest.fn(async () => {
            await sleep(500);
        });
        queue.process('slowConcurrentTask', slowHandler);

        const jobA = await queue.enqueue('slowConcurrentTask', { job: 'A' });
        const jobB = await queue.enqueue('slowConcurrentTask', { job: 'B' });
        const jobC = await queue.enqueue('slowConcurrentTask', { job: 'C' });

        // Wait ~100ms to let them start
        await sleep(100);

        let [a, b, c] = await Promise.all([
            Job.findById(jobA._id),
            Job.findById(jobB._id),
            Job.findById(jobC._id),
        ]);

        const statuses = [a.status, b.status, c.status];
        const processingCount = statuses.filter((s) => s === 'processing').length;
        const queuedCount = statuses.filter((s) => s === 'queued').length;

        expect(processingCount).toBe(2);
        expect(queuedCount).toBe(1);

        // Wait ~600ms => first two should complete
        await sleep(600);

        // Then third one processes
        await sleep(600);

        [a, b, c] = await Promise.all([
            Job.findById(jobA._id),
            Job.findById(jobB._id),
            Job.findById(jobC._id),
        ]);

        expect(a.status).toBe('completed');
        expect(b.status).toBe('completed');
        expect(c.status).toBe('completed');
        expect(slowHandler).toHaveBeenCalledTimes(3);
    });

    // --------------------------------------------------------------------------
    // Job Counts
    // --------------------------------------------------------------------------
    test('should update job counts after enqueue and completion', async () => {
        queue.pause();
        expect(queue.jobCounts.queued).toBe(0);

        const job = await queue.enqueue('successTask', { foo: 'bar' });
        await sleep(100); // let _updateJobCounts run
        expect(queue.jobCounts.queued).toBe(1);

        queue.resume();
        await sleep(300);

        expect(queue.jobCounts.completed).toBe(1);
        expect(queue.jobCounts.queued).toBe(0);
    });
});
