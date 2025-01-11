/**
 * @file queueOrphanedJobs.test.js
 * A dedicated test suite for orphaned job recovery in "requeueAll" mode.
 */

jest.setTimeout(30000); // Increase test timeout to 30s

const mongoose = require('mongoose');
const { connectDB, disconnectDB, clearDB } = require('../setup/test-setup');
const Queue = require('../../lib/queue');
const Job = require('../../lib/models/job');
const DeadLetterJob = require('../../lib/models/deadLetterJob');

function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

describe('Orphaned Job Recovery - requeueAll', () => {
    let queue;

    beforeAll(async () => {
        // 1) Connect once to in-memory Mongo
        await connectDB();
    });

    afterAll(async () => {
        // 2) Disconnect after all tests in this file
        await disconnectDB();
    });

    beforeEach(async () => {
        // Clear DB + any mock logs before each test
        await clearDB();
        jest.clearAllMocks();
    });

    test('should re-queue orphaned jobs stuck in processing => eventually completed', async () => {
        const orphanedJob = await Job.create({
            taskName: 'successTask',
            data: { reason: 'simulate crash' },
            status: 'processing',
            attempts: 1,
            maxRetries: 3,
        });

        let doc = await Job.findById(orphanedJob._id);
        expect(doc.status).toBe('processing');

        queue = new Queue(mongoose.connection, {
            jobModel: Job,
            deadLetterJobModel: DeadLetterJob,
            concurrency: 1,
            orphanedJobs: { strategy: 'requeueAll' },
        });

        queue.process('successTask', async () => {
            console.log('Handler executed for successTask');
        });

        try {
            await queue.start();

            // Check intermediate status
            doc = await Job.findById(orphanedJob._id);
            console.log('After start():', doc.status);
            expect(doc.status).toBe('queued');

            // Poll for completion
            let finalDoc = null;
            for (let i = 0; i < 20; i++) {
                await sleep(500);
                finalDoc = await Job.findById(orphanedJob._id);
                console.log(`Poll ${i + 1}: Job status = ${finalDoc?.status}`);
                if (finalDoc?.status === 'completed') break;
            }

            expect(finalDoc).toBeTruthy();
            expect(finalDoc.status).toBe('completed');
        } finally {
            await queue.stop();
        }
    });

});
