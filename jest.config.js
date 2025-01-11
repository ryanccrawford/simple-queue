module.exports = {
    preset: '@shelf/jest-mongodb',
    testEnvironment: 'node',
    testMatch: ['**/__tests__/**/*.test.js'],
    coveragePathIgnorePatterns: ['/node_modules/', '/__tests__/'],
    transformIgnorePatterns: [
        'node_modules/(?!(serialize-error)/)', // allow transforming this ESM dep
    ],
    reporters: [
        'default',
        [
            'jest-html-reporters',
            {
                publicPath: './coverage',
                filename: 'report.html',
                expand: true,
            },
        ],
    ],
};