const mongoose = require('mongoose');
const EventEmitter = require('events');
const Job = require('../models/job');
const DeadLetterJob = require('../models/deadLetterJob');

class Queue extends EventEmitter {
    constructor(dbConnection, options = {}) {
        super();
        this.dbConnection = dbConnection;
        this.retryOptions = {
            retries: options.retries || 3,
            interval: options.interval || 1000,
            backoffFactor: options.backoffFactor || 2,
        };
        this.handlers = {};
        this.changeStream = null;
        this.isPaused = false;
        this.jobCounts = {
            queued: 0,
            processing: 0,
            completed: 0,
            failed: 0,
            retrying: 0,
            canceled: 0,
        };
    }

    async _updateJobCounts() {
        this.jobCounts.queued = await Job.countDocuments({ status: 'queued' });
        this.jobCounts.processing = await Job.countDocuments({
            status: 'processing',
        });
        this.jobCounts.completed = await Job.countDocuments({
            status: 'completed',
        });
        this.jobCounts.failed = await Job.countDocuments({ status: 'failed' });
        this.jobCounts.retrying = await Job.countDocuments({
            status: 'retrying',
        });
        this.jobCounts.canceled = await Job.countDocuments({
            status: 'canceled',
        });
        this.emit('jobCountsUpdated', this.jobCounts);
    }

    async enqueue(taskName, data, options = {}) {
        const job = await Job.create({
            taskName,
            data,
            priority: options.priority || 0,
            maxRetries: options.maxRetries || this.retryOptions.retries,
            retryInterval: options.retryInterval || this.retryOptions.interval,
            backoffFactor: options.backoffFactor || this.retryOptions.backoffFactor,
        });
        await job.save();
        await this._updateJobCounts();
        return job;
    }

    async dequeue() {
        const job = await Job.findOneAndUpdate(
            { status: 'queued' },
            { $set: { status: 'processing', attempts: 1, lastAttempt: new Date() } },
            { sort: { priority: -1, createdAt: 1 }, new: true } // Prioritize higher priority and older jobs
        );

        return job;
    }

    process(taskName, handler) {
        this.handlers[taskName] = handler;
    }

    async listen() {
        const pipeline = [
            {
                $match: {
                    $or: [
                        { operationType: 'insert' },
                        {
                            operationType: 'update',
                            'updateDescription.updatedFields.status': 'queued',
                        },
                    ],
                },
            },
        ];

        this.changeStream = Job.watch(pipeline, {
            fullDocument: 'updateLookup',
        });

        this.changeStream.on('change', async (next) => {
            if (this.isPaused) return;

            const operation = next.operationType;
            let job = null;
            if (operation === 'insert') {
                job = next.fullDocument;
            } else {
                job = await Job.findById(next.documentKey._id);
            }

            if (job && job.status === 'queued') {
                this.emit('job', job); // Emit event for new job
                await this._processJob(job);
            }
        });

        this.changeStream.on('error', (error) => {
            console.error('Change stream error:', error);
            this.emit('error', error); // Emit error event
        });
    }

    async _processJob(job) {
        const handler = this.handlers[job.taskName];
        if (!handler) {
            console.error(`No handler found for task: ${job.taskName}`);
            return;
        }

        try {
            await handler(job.data);
            await this._markAsComplete(job._id);
        } catch (error) {
            await this._handleJobFailure(job, error);
        }
    }

    async _handleJobFailure(job, error) {
        const retryDelay = job.retryInterval * Math.pow(job.backoffFactor, job.attempts - 1); // Exponential backoff
        const retryLimit = job.retryInterval * Math.pow(job.backoffFactor, job.maxRetries);

        if (job.attempts < job.maxRetries && retryDelay <= retryLimit) {
            job.attempts++;
            job.lastError = error;
            job.status = 'retrying';
            job.nextRunAt = new Date(Date.now() + retryDelay); // Schedule next run
            await job.save();

            console.warn(
                `Job ${job._id} failed, retrying in ${retryDelay}ms. Attempt ${job.attempts} of ${job.maxRetries}.`
            );
            setTimeout(() => this._requeue(job._id), retryDelay);
        } else {
            await this._moveToDeadLetterQueue(
                job,
                error,
                `Exceeded max retries or retry delay limit`
            );
        }
    }

    async _markAsComplete(jobId) {
        await Job.updateOne(
            { _id: jobId },
            {
                $set: {
                    status: 'completed',
                    lastError: null,
                    nextRunAt: null,
                },
            }
        );
        this.emit('jobComplete', jobId); // Emit event
        await this._updateJobCounts();
    }

    async _markAsFailed(jobId, error) {
        await Job.updateOne(
            { _id: jobId },
            {
                $set: {
                    status: 'failed',
                    lastError: error,
                    nextRunAt: null,
                },
            }
        );
        this.emit('jobFailed', jobId, error);
        await this._updateJobCounts();
    }

    async _requeue(jobId) {
        const job = await Job.findById(jobId);
        if (job && job.status === 'retrying') {
            job.status = 'queued';
            job.nextRunAt = null;
            await job.save();
            await this._updateJobCounts();
            this.emit('jobRequeued', jobId);
        }

    }

    async _moveToDeadLetterQueue(job, error, reason) {
        try {
            const now = new Date();
            const { serializeError } = await import('serialize-error');
            let errorToLog = error instanceof Error ? serializeError(error) : error;
            await DeadLetterJob.create({
                jobId: job._id,
                originalJob: job,
                error: errorToLog,
                reason,
                firstOccurred: now,
                lastOccurred: now,
                attempts: job.attempts,
                status: 'pending',
                component: 'Queue',
            });
            console.info(
                `Moved job ${job._id} to dead-letter queue. Reason: ${reason}`
            );
            await Job.deleteOne({ _id: job._id });
        } catch (err) {
            console.error(
                `Error moving job ${job._id} to dead-letter queue:`,
                err
            );
        }
    }

    async cancelJob(jobId) {
        try {
            const job = await Job.findByIdAndUpdate(jobId, {
                $set: { status: 'canceled', lastError: null, nextRunAt: null }
            }, null);
            if (job) {
                this.emit('jobCanceled', jobId);
                console.info(`Job ${jobId} canceled.`);
            } else {
                console.warn(`Job ${jobId} not found for cancellation.`);
            }
        } catch (error) {
            console.error(`Error canceling job ${jobId}:`, error);
            this.emit('error', error);
        }
        await this._updateJobCounts();
    }

    pause() {
        this.isPaused = true;
        console.info('Queue paused.');
    }

    async resume() {
        this.isPaused = false;
        console.info('Queue resumed.');

        const queuedJobs = await Job.find({ status: 'queued' }, null, null).sort({
            priority: -1,
            createdAt: 1,
        });
        for (const job of queuedJobs) {
            await this._processJob(job);
        }
    }
}

module.exports = Queue;