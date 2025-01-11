const mongoose = require('mongoose');
const { MongoMemoryReplSet } = require('mongodb-memory-server');

let mongod;

module.exports = {
    async connectDB() {
        try {
            mongod = await MongoMemoryReplSet.create({ replSet: { count: 3 } });
            const mongoUri = mongod.getUri();

            await mongoose.connect(mongoUri, {});

            console.log('Connected to in-memory MongoDB replica set');
        } catch (error) {
            console.error('Error connecting to in-memory MongoDB:', error);
            process.exit(1); // Exit if cannot connect to test database
        }
    },

    async disconnectDB() {
        try {
            await mongoose.disconnect();
            await mongod.stop();
            console.log('Disconnected from in-memory MongoDB');
        } catch (error) {
            console.error('Error disconnecting from in-memory MongoDB:', error);
            process.exit(1); // Exit if cannot disconnect from test database
        }
    },

    async clearDB() {
        const collections = mongoose.connection.collections;

        for (const key in collections) {
            const collection = collections[key];
            try {
                await collection.deleteMany();
            } catch (error) {
                console.error(`Error clearing collection ${collection.name}:`, error);
            }
        }
    },
};