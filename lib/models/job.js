const mongoose = require('mongoose');

const JobSchema = new mongoose.Schema({
    taskName: { type: String, required: true },
    data: { type: mongoose.Schema.Types.Mixed },
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
    lastError: { type: mongoose.Schema.Types.Mixed },
    createdAt: { type: Date, default: Date.now },
    updatedAt: { type: Date, default: Date.now },
    nextRunAt: { type: Date, default: null },
}, {
    timestamps: true,
    toJSON: { virtuals: true },
    toObject: { virtuals: true }
});

const Job = mongoose.model('Job', JobSchema);

module.exports = Job;