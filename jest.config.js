export default {
  testEnvironment: "node",
  transform: {},
  testMatch: ["**/*.test.js"],
  collectCoverageFrom: ["index.js"],
  coveragePathIgnorePatterns: ["/node_modules/"],
  testTimeout: 30000,
  coverageThreshold: {
    global: {
      branches: 80,
      functions: 80,
      lines: 80,
      statements: 80,
    },
  },
};
