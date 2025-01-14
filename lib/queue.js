/**
 * @file Queue.js
 * A "best-in-class", opinionated queue using MongoDB change streams.
 */

const EventEmitter = require('events');


/**
 * @typedef {import('mongoose').Connection} MongooseConnection
 * @typedef {import('mongoose').Model} MongooseModel
 */

/**
 * A robust queue built around MongoDB change streams and Mongoose.
 */
class Queue extends EventEmitter {
    /**
     * Creates a new Queue instance.
     * @param {MongooseConnection} dbConnection - Mongoose connection (must be open).
     * @param {Object} options
     * @param {MongooseModel} options.jobModel - Mongoose model for the main jobs.
     * @param {MongooseModel} options.deadLetterJobModel - Mongoose model for dead-letter jobs.
     * @param {number} [options.retries=3] - Default number of retries per job.
     * @param {number} [options.interval=1000] - Base retry interval in ms.
     * @param {number} [options.backoffFactor=2] - Exponential backoff multiplier.
     * @param {number} [options.concurrency=5] - Max number of jobs to process concurrently in this process.
     * @param {string} [options.jobCollectionName='jobs'] - Name of the collection for jobs.
     * @param {string} [options.deadLetterJobCollectionName='deadLetterJobs']
     * @param {Function} [options.jobProcessor] - If you want a “global” job processor wrapper.
     * @param {Object} [options.logger=console] - A custom logger with methods debug, info, warn, error, etc.
     *
     * @param {Object} [options.orphanedJobs] - Config for orphaned job recovery
     * @param {string} [options.orphanedJobs.strategy='none'] - 'none'|'requeueAll'|'failAll'|'timeLimit'
     * @param {number} [options.orphanedJobs.timeLimitMs=3600000] - If `strategy='timeLimit'`, how long (ms) to let 'processing' stand before orphaned
     * @param {string} [options.orphanedJobs.timeLimitAction='requeue'] - If `strategy='timeLimit'`, do we 'requeue', 'fail', or 'dlq' orphaned jobs?
     * @param {string} [options.orphanedJobs.failAllAction='dlq'] - If `strategy='failAll'`, do we just mark them 'failed' or move them to 'dlq'?
     *
     */
    constructor(dbConnection, options = {}) {
        super();

        const {
            jobModel,
            deadLetterJobModel,
            retries = 3,
            interval = 1000,
            backoffFactor = 2,
            concurrency = 5,
            jobCollectionName = 'jobs',
            deadLetterJobCollectionName = 'deadLetterJobs',
            jobProcessor = null,
            logger = console,
            orphanedJobs = {},
        } = options;

        const {
            strategy = 'none',
            timeLimitMs = 3600000,         // default 1 hour
            timeLimitAction = 'requeue',   // 'requeue'|'fail'|'dlq'
            failAllAction = 'dlq',         // if strategy='failAll', do 'failed' or 'dlq'
        } = orphanedJobs;

        if (!jobModel) {
            throw new Error('Queue requires a jobModel (MongooseModel).');
        }
        if (!deadLetterJobModel) {
            throw new Error('Queue requires a deadLetterJobModel (MongooseModel).');
        }
        if (!dbConnection || typeof dbConnection.model !== 'function') {
            throw new QueueError('A valid Mongoose connection is required.', { dbConnection });
        }


        this.dbConnection = dbConnection;
        this.jobModel = jobModel;
        this.deadLetterJobModel = deadLetterJobModel;
        this.jobCollectionName = jobCollectionName;
        this.deadLetterJobCollectionName = deadLetterJobCollectionName;

        this.retryOptions = { retries, interval, backoffFactor };

        // For concurrency within this single Node process
        this.concurrency = concurrency;
        this.activeJobs = 0;
        this.backlog = [];

        // Optionally wrap each job handler in a "jobProcessor"
        this.jobProcessor = jobProcessor;

        // For controlling the change stream
        this.changeStream = null;
        this.isPaused = false;

        // Task handler registry:  { taskName: (jobData) => Promise<void> }
        this.handlers = {};

        // Useful for dashboards or metrics
        this.jobCounts = {
            queued: 0,
            processing: 0,
            completed: 0,
            failed: 0,
            retrying: 0,
            canceled: 0,
        };

        // Custom or default logger
        this.logger = logger;

        // Orphaned job recovery config
        this.orphanedStrategy = strategy;         // 'none'|'requeueAll'|'failAll'|'timeLimit'
        this.orphanedTimeLimitMs = timeLimitMs;   // If 'timeLimit'
        this.orphanedTimeLimitAction = timeLimitAction;   // 'requeue'|'fail'|'dlq'
        this.failAllAction = failAllAction;       // 'dlq' or 'failed'

        this.logger.info(
            `[Queue] Created with concurrency=${this.concurrency}, orphanedStrategy=${this.orphanedStrategy}.`
        );
    }

    // --------------------------------------------------------------------------
    // Public API
    // --------------------------------------------------------------------------

    /**
     * Registers a handler for a given taskName.
     * @param {string} taskName
     * @param {Function} handler - async function that processes the job data.
     */
    process(taskName, handler) {
        if (typeof handler !== 'function') {
            throw new Error(`Handler for task "${taskName}" must be a function`);
        }
        this.handlers[taskName] = handler;
        this.logger.info(`[Queue] Registered handler for task "${taskName}".`);
    }

    /**
     * Enqueues a new job in "queued" status.
     * @param {string} taskName
     * @param {Object} data
     * @param {Object} [options]
     * @param {number} [options.priority=0]
     * @param {number} [options.maxRetries=this.retryOptions.retries]
     * @param {number} [options.retryInterval=this.retryOptions.interval]
     * @param {number} [options.backoffFactor=this.retryOptions.backoffFactor]
     * @returns {Promise<Object>} The created job document.
     */
    async enqueue(taskName, data, options = {}) {
        try {
            const {
                priority = 0,
                maxRetries = this.retryOptions.retries,
                retryInterval = this.retryOptions.interval,
                backoffFactor = this.retryOptions.backoffFactor,
            } = options;

            const job = await this.jobModel.create({
                taskName,
                data,
                priority,
                maxRetries,
                retryInterval,
                backoffFactor,
                status: 'queued',
                attempts: 0,
                lastError: null,
                nextRunAt: null,
            });

            this.logger.debug(`[Queue] Enqueued job ${job._id} (taskName="${taskName}").`);
            await this._updateJobCounts();
            return job;
        } catch (error) {
            this._handleError(error, 'enqueue()');
            throw error;
        }
    }

    /**
     * Cancels a job by setting its status to "canceled".
     * @param {string|import('mongoose').Types.ObjectId} jobId
     */
    async cancelJob(jobId) {
        try {
            const job = await this.jobModel.findByIdAndUpdate(jobId, {
                $set: {
                    status: 'canceled',
                    lastError: null,
                    nextRunAt: null,
                },
            }, { new: true });

            if (job) {
                this.logger.info(`[Queue] Canceled job ${jobId}.`);
                this.emit('jobCanceled', jobId);
            } else {
                this.logger.warn(`[Queue] Job ${jobId} not found for cancellation.`);
            }
            await this._updateJobCounts();
        } catch (error) {
            this._handleError(error, `cancelJob(${jobId})`);
        }
    }

    /**
     * Pauses processing of new jobs (does not stop the change stream).
     */
    pause() {
        this.isPaused = true;
        this.logger.info('[Queue] Queue paused.');
    }

    /**
     * Resumes processing of queued jobs.
     * Re-dispatches all queued jobs, respecting concurrency.
     */
    async resume() {
        this.isPaused = false;
        this.logger.info('[Queue] Queue resumed.');

        try {
            const queuedJobs = await this.jobModel
                .find({ status: 'queued' })
                .sort({ priority: -1, createdAt: 1 });

            for (const job of queuedJobs) {
                this._dispatchJob(job);
            }
        } catch (error) {
            this._handleError(error, 'resume()');
        }
    }

    /**
     * Starts the queue: opens the change stream if not already open, and begins
     * listening for insert/update events that set jobs to "queued".
     */
    async start() {
        try {
            // First handle orphaned jobs according to strategy
            await this._recoverOrphanedJobs();

            if (!this.changeStream) {
                await this._startChangeStream();
            }
            this.logger.info('[Queue] Queue started listening for changes.');
        } catch (error) {
            this._handleError(error, 'start()');
        }
    }

    /**
     * Stops the queue: closes the change stream, preventing new jobs from being processed.
     */
    async stop() {
        try {
            if (this.changeStream) {
                await this._stopChangeStream();
                this.logger.info('[Queue] Queue stopped listening for changes.');
            }
        } catch (error) {
            this._handleError(error, 'stop()');
        }
    }

    // --------------------------------------------------------------------------
    // Private / Internal Methods
    // --------------------------------------------------------------------------

    /**
     * Runs all the countDocuments() queries in parallel and updates jobCounts.
     * Emits 'jobCountsUpdated' upon completion.
     */
    async _updateJobCounts() {
        try {
            const [
                queued,
                processing,
                completed,
                failed,
                retrying,
                canceled,
            ] = await Promise.all([
                this.jobModel.countDocuments({ status: 'queued' }),
                this.jobModel.countDocuments({ status: 'processing' }),
                this.jobModel.countDocuments({ status: 'completed' }),
                this.jobModel.countDocuments({ status: 'failed' }),
                this.jobModel.countDocuments({ status: 'retrying' }),
                this.jobModel.countDocuments({ status: 'canceled' }),
            ]);

            this.jobCounts = {
                queued,
                processing,
                completed,
                failed,
                retrying,
                canceled,
            };

            this.emit('jobCountsUpdated', this.jobCounts);
        } catch (error) {
            this._handleError(error, '_updateJobCounts()');
        }
    }

    /**
     * Opens a MongoDB change stream to watch for new or re-queued jobs.
     * - When an 'insert' or relevant 'update' is seen, we dispatch the job.
     */
    async _startChangeStream() {
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

        this.changeStream = this.jobModel.watch(pipeline, {
            fullDocument: 'updateLookup',
        });

        this.changeStream.on('change', async (changeEvent) => {
            this.logger.info(`[Queue] Change event detected: ${JSON.stringify(changeEvent)}`);

            if (this.isPaused) return; // Do nothing if queue is paused

            try {
                // Always fetch a fresh Mongoose doc, regardless of insert or update
                const jobDoc = await this.jobModel.findById(changeEvent.documentKey._id);
                if (!jobDoc) {
                    // Job might have been deleted or something else
                    return;
                }

                if (jobDoc.status === 'queued') {
                    this.emit('job', jobDoc);
                    await this._dispatchJob(jobDoc);
                }
            } catch (err) {
                this._handleError(err, 'changeStream');
            }
        });

        this.changeStream.on('error', (error) => {
            this._handleError(error, 'changeStream');
        });
    }

    /**
     * Closes the change stream if open.
     */
    async _stopChangeStream() {
        if (this.changeStream) {
            await this.changeStream.close();
            this.changeStream = null;
        }
    }

    /**
     * Removes Expired Jobs
     *
     */
    async _removeExpiredJobs() {
        const now = new Date();
        const result = await this.jobModel.deleteMany({ nextRunAt: { $lt: now } });
        this.logger.info(`[Queue] Removed ${result.deletedCount} expired jobs.`);
    }

    // --------------------------------------------------------------------------
    // Concurrency & Dispatch
    // --------------------------------------------------------------------------

    /**
     * Attempts to dispatch a job for immediate processing if a concurrency slot is free.
     * Otherwise, adds the job to a backlog.
     * @param {Object} jobDoc
     */
    _dispatchJob(jobDoc) {
        this.logger.info(`[Queue] Dispatching job ${jobDoc._id} (status=${jobDoc.status}).`);

        if (this.activeJobs >= this.concurrency) {
            this.logger.debug(
                `[Queue] Reached concurrency limit. Putting job ${jobDoc._id} in backlog.`
            );
            this.backlog.push(jobDoc);
            return;
        }

        this.activeJobs++;
        this.logger.debug(
            `[Queue] Processing job ${jobDoc._id} now (activeJobs=${this.activeJobs}).`
        );

        // Always run _processJob asynchronously
        this._processJob(jobDoc).finally(() => {
            // When done (success or failure), release the concurrency slot
            this.activeJobs--;

            if (this.backlog.length > 0) {
                const next = this.backlog.shift();
                this.logger.debug(
                    `[Queue] Dispatching job ${next._id} from backlog. (activeJobs=${this.activeJobs})`
                );
                this._dispatchJob(next);
            }
        });
    }

    // --------------------------------------------------------------------------
    // Job Execution & State Transitions
    // --------------------------------------------------------------------------

    /**
     * Processes a job by invoking its handler, then marking complete or handling failure.
     * @param {Object} jobDoc
     */
    async _processJob(jobDoc) {
        this.logger.info(`[Queue] Processing job ${jobDoc._id} (taskName=${jobDoc.taskName}).`);

        // Mark status=processing if not already
        if (jobDoc.status !== 'processing') {
            try {
                jobDoc.status = 'processing';
                jobDoc.attempts = (jobDoc.attempts || 0) + 1;
                jobDoc.lastAttempt = new Date();
                await jobDoc.save();
            } catch (error) {
                this._handleError(error, `_processJob() setting status=processing for ${jobDoc._id}`);
                return;
            }
        }

        const handler = this.handlers[jobDoc.taskName];
        if (!handler) {
            this.logger.error(
                `[Queue] No handler registered for taskName="${jobDoc.taskName}".`
            );
            await this._markAsFailed(jobDoc._id, new Error('No handler registered.'));
            return;
        }

        try {
            // If you have a global jobProcessor, call it here
            if (this.jobProcessor) {
                await this.jobProcessor(jobDoc, handler);
            } else {
                await handler(jobDoc.data);
            }

            await this._markAsComplete(jobDoc._id);
        } catch (error) {
            await this._handleFailure(jobDoc, error);
        }
    }

    /**
     * Handles job failure. Decides whether to retry or move to dead-letter queue.
     * @param {Object} jobDoc
     * @param {any} error
     */
    async _handleFailure(jobDoc, error) {
        const maxRetries = jobDoc.maxRetries || this.retryOptions.retries;
        const attempts = jobDoc.attempts || 1;
        const retryDelay = (jobDoc.retryInterval || this.retryOptions.interval) *
            Math.pow((jobDoc.backoffFactor || this.retryOptions.backoffFactor), attempts - 1);

        // If we can still retry:
        if (attempts < maxRetries) {
            await this._scheduleRetry(jobDoc, error, retryDelay);
        } else {
            // Exceeded max attempts
            await this._moveToDeadLetterQueue(jobDoc, error, 'Max retries exceeded');
        }
    }

    /**
     * Schedules a retry for a job, marking status=retrying and setting a nextRunAt date.
     * Then uses a setTimeout to re-queue the job after the delay.
     * @param {Object} jobDoc
     * @param {any} error
     * @param {number} delayMs
     */
    async _scheduleRetry(jobDoc, error, delayMs) {
        try {
            jobDoc.status = 'retrying';
            jobDoc.lastError = error;
            jobDoc.nextRunAt = new Date(Date.now() + delayMs);
            await jobDoc.save();

            this.logger.warn(
                `[Queue] Job ${jobDoc._id} failed; retrying in ${delayMs}ms (attempt=${jobDoc.attempts}).`
            );

            setTimeout(async () => {
                // Attempt to re-queue
                await this._requeue(jobDoc._id);
            }, delayMs);
        } catch (saveErr) {
            this._handleError(saveErr, `_scheduleRetry() - saving job ${jobDoc._id}`);
        }
    }

    /**
     * Requeues a job that is in "retrying" status by setting status="queued".
     * @param {string|import('mongoose').Types.ObjectId} jobId
     */
    async _requeue(jobId) {
        try {
            const job = await this.jobModel.findById(jobId);
            if (!job) {
                this.logger.warn(`[Queue] Attempted to requeue missing job ${jobId}.`);
                return;
            }
            if (job.status !== 'retrying') {
                this.logger.debug(
                    `[Queue] Job ${jobId} not in "retrying" state, skipping requeue.`
                );
                return;
            }

            job.status = 'queued';
            job.nextRunAt = null;
            await job.save();

            this.logger.debug(`[Queue] Requeued job ${jobId}.`);
            this.emit('jobRequeued', jobId);
            await this._updateJobCounts();

            // Immediately dispatch it if not paused
            if (!this.isPaused) {
                this._dispatchJob(job);
            }
        } catch (error) {
            this._handleError(error, `_requeue(${jobId})`);
        }
    }

    /**
     * Moves a job to the dead-letter queue, removing it from the main collection.
     * @param {Object} jobDoc
     * @param {any} error
     * @param {string} reason
     */
    async _moveToDeadLetterQueue(jobDoc, error, reason) {
        try {
            const { serializeError } = await import('serialize-error');
            const now = new Date();
            const serializedErr = error instanceof Error ? serializeError(error) : error;

            // Create a dead-letter job
            await this.deadLetterJobModel.create({
                jobId: jobDoc._id,
                originalJob: jobDoc.toObject(),
                error: serializedErr,
                reason,
                firstOccurred: now,
                lastOccurred: now,
                attempts: jobDoc.attempts || 1,
                status: 'pending',
                component: 'Queue',
            });

            this.logger.info(
                `[Queue] Moved job ${jobDoc._id} to dead-letter queue. Reason: ${reason}`
            );

            // Remove from main queue
            await this.jobModel.deleteOne({ _id: jobDoc._id });
            await this._updateJobCounts();
        } catch (dlqErr) {
            this._handleError(dlqErr, `_moveToDeadLetterQueue(${jobDoc._id})`);
        }
    }

    /**
     * Marks a job as completed (status="completed").
     * @param {string|import('mongoose').Types.ObjectId} jobId
     */
    async _markAsComplete(jobId) {
        try {
            await this.jobModel.updateOne(
                { _id: jobId },
                {
                    $set: {
                        status: 'completed',
                        lastError: null,
                        nextRunAt: null,
                    },
                }
            );
            this.logger.debug(`[Queue] Job ${jobId} marked as completed.`);
            this.emit('jobComplete', jobId);
            await this._updateJobCounts();
        } catch (error) {
            this._handleError(error, `_markAsComplete(${jobId})`);
        }
    }

    /**
     * Marks a job as failed (status="failed").
     * @param {string|import('mongoose').Types.ObjectId} jobId
     * @param {any} error
     */
    async _markAsFailed(jobId, error) {
        try {
            await this.jobModel.updateOne(
                { _id: jobId },
                {
                    $set: {
                        status: 'failed',
                        lastError: error,
                        nextRunAt: null,
                    },
                }
            );
            this.logger.warn(`[Queue] Job ${jobId} marked as failed.`);
            this.emit('jobFailed', jobId, error);
            await this._updateJobCounts();
        } catch (err) {
            this._handleError(err, `_markAsFailed(${jobId})`);
        }
    }

    // --------------------------------------------------------------------------
    // Error Handling
    // --------------------------------------------------------------------------

    /**
     * Centralized error handling: logs the error and emits 'error' event.
     * @param {Error|any} err
     * @param {string} context - short identifier for logs
     */
    _handleError(err, context) {
        // Use the logger's "error" method
        this.logger.error(`[Queue] ERROR in ${context}:`, err);
        this.emit('error', err);
    }

    /**
     * Recovers or handles orphaned jobs that were in "processing" state.
     * Based on user-selected strategy: 'none'|'requeueAll'|'failAll'|'timeLimit'.
     */
    async _recoverOrphanedJobs() {
        try {
            switch (this.orphanedStrategy) {
                case 'none':
                    this.logger.info('[Queue] Orphaned job recovery: none. Skipping.');
                    return;

                case 'requeueAll':
                    await this._requeueAllProcessing();
                    return;

                case 'failAll':
                    await this._failAllProcessing();
                    return;

                case 'timeLimit':
                    await this._timeLimitRecovery();
                    return;

                default:
                    this.logger.warn(`[Queue] Unknown orphanedStrategy=${this.orphanedStrategy}. Skipping.`);
                    return;
            }
        } catch (error) {
            this.logger.error('[Queue] Error in _recoverOrphanedJobs:', error);
            // We won't fail the queue entirely; just log
        }
    }

    /**
     * Re-queue ALL jobs stuck in processing => set status='queued'.
     */
    async _requeueAllProcessing() {
        const result = await this.jobModel.updateMany(
            { status: 'processing' },
            {
                $set: {
                    status: 'queued',
                    nextRunAt: null,
                    lastError: 'Orphaned job re-queued on startup',
                },
            }
        );

        this.logger.warn(`[Queue] Requeued ${result.modifiedCount} jobs that were stuck in processing.`);
        const queuedJobs = await this.jobModel.find({ status: 'queued' });
        this.logger.info(`[Queue] Queued jobs after requeue: ${queuedJobs.map(job => job._id).join(', ')}`);
        for (const job of queuedJobs) {
            this._dispatchJob(job);
        }
    }


    /**
     * Fails ALL jobs stuck in processing => either mark as 'failed' or move them to DLQ,
     * depending on user config failAllAction = 'dlq' | 'failed'.
     */
    async _failAllProcessing() {
        if (this.failAllAction === 'failed') {
            // Just mark them as 'failed'
            const result = await this.jobModel.updateMany(
                { status: 'processing' },
                {
                    $set: {
                        status: 'failed',
                        lastError: 'Orphaned job: system crashed before completion',
                        nextRunAt: null,
                    },
                }
            );
            this.logger.warn(`[Queue] Marked ${result.modifiedCount} orphaned 'processing' jobs as failed.`);
        } else {
            // Move them to the dead-letter queue (dlq)
            // We find them, then call our `_moveToDeadLetterQueue` for each
            const processingJobs = await this.jobModel.find({ status: 'processing' });
            let movedCount = 0;

            for (const job of processingJobs) {
                await this._moveToDeadLetterQueue(
                    job,
                    'Orphaned job: system crashed before completion',
                    'Orphaned job - failAll'
                );
                movedCount++;
            }
            this.logger.warn(`[Queue] Moved ${movedCount} orphaned 'processing' jobs to dead-letter queue.`);
        }
    }

    /**
     * Handles 'timeLimit' strategy => only re-queue/fail/dlq jobs older than timeLimitMs.
     */
    async _timeLimitRecovery() {
        const cutoff = new Date(Date.now() - this.orphanedTimeLimitMs);

        const stuckJobs = await this.jobModel.find({
            status: 'processing',
            lastAttempt: { $lt: cutoff },
        });

        if (stuckJobs.length === 0) {
            this.logger.info(`[Queue] No 'processing' jobs older than ${this.orphanedTimeLimitMs}ms.`);
            return;
        }

        this.logger.warn(
            `[Queue] Found ${stuckJobs.length} 'processing' jobs older than ${this.orphanedTimeLimitMs} ms.`
        );

        let count = 0;

        if (this.orphanedTimeLimitAction === 'requeue') {
            for (const job of stuckJobs) {
                job.status = 'queued';
                job.nextRunAt = null;
                job.lastError = 'Orphaned job (timeLimit) re-queued on startup.';
                await job.save();
                count++;
            }
            this.logger.warn(`[Queue] Requeued ${count} orphaned jobs (timeLimit).`);
        } else if (this.orphanedTimeLimitAction === 'fail') {
            for (const job of stuckJobs) {
                job.status = 'failed';
                job.lastError = 'Orphaned job (timeLimit) => forcibly failed.';
                job.nextRunAt = null;
                await job.save();
                count++;
            }
            this.logger.warn(`[Queue] Marked ${count} orphaned jobs as failed (timeLimit).`);
        } else if (this.orphanedTimeLimitAction === 'dlq') {
            for (const job of stuckJobs) {
                await this._moveToDeadLetterQueue(
                    job,
                    'Orphaned job (timeLimit) => forcibly DLQed.',
                    'orphanedJob-timeLimit'
                );
                count++;
            }
            this.logger.warn(`[Queue] Moved ${count} orphaned jobs to DLQ (timeLimit).`);
        } else {
            this.logger.warn(
                `[Queue] Unknown timeLimitAction="${this.orphanedTimeLimitAction}". Doing nothing.`
            );
        }
    }

}

module.exports = Queue;
