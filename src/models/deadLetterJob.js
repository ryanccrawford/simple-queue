const mongoose = require('mongoose');

const DeadLetterJobSchema = new mongoose.Schema({
    jobId: { type: mongoose.Schema.Types.ObjectId, required: true, ref: 'Job' },
    originalJob: { type: mongoose.Schema.Types.Mixed }, // Store the original job data
    error: { type: mongoose.Schema.Types.Mixed },
    reason: { type: String },
    firstOccurred: { type: Date },
    lastOccurred: { type: Date },
    attempts: { type: Number },
    status: { type: String, default: 'pending' },
    component: { type: String },
    createdAt: { type: Date, default: Date.now },
});

const DeadLetterJob = mongoose.model('DeadLetterJob', DeadLetterJobSchema);

module.exports = DeadLetterJob;