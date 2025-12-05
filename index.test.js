import { jest } from "@jest/globals";

// Mock MongoDB
const mockCollection = {
  insertOne: jest.fn(),
  findOne: jest.fn(),
  findOneAndUpdate: jest.fn(),
  updateOne: jest.fn(),
  createIndex: jest.fn(),
};

const mockDb = {
  collection: jest.fn(() => mockCollection),
};

const mockClient = {
  db: jest.fn(() => mockDb),
  close: jest.fn(),
};

const MockMongoClient = jest.fn(() => mockClient);

jest.unstable_mockModule("mongodb", () => ({
  MongoClient: MockMongoClient,
}));

// Mock timers
let mockTime = 1000000;
const originalDate = global.Date;
const originalSetTimeout = global.setTimeout;

global.Date = class extends originalDate {
  constructor(...args) {
    if (args.length === 0) {
      super(mockTime);
    } else {
      super(...args);
    }
  }

  static now() {
    return mockTime;
  }
};

global.Date.parse = originalDate.parse;
global.Date.UTC = originalDate.UTC;

global.setTimeout = jest.fn((fn) => {
  return originalSetTimeout(fn, 0); // Execute immediately for tests
});

// Import after mocking
const { Bluestreak, WorkflowNotFound, HandlerNotFound, WaitTimeout, WorkflowAlreadyStarted } = await import("./index.js");

describe("Bluestreak", () => {
  let bluestreak;

  beforeEach(() => {
    // Reset mocks
    jest.clearAllMocks();
    mockTime = 1000000;

    // Setup default mock behaviors
    mockCollection.createIndex.mockResolvedValue(undefined);
    mockCollection.insertOne.mockResolvedValue({ acknowledged: true });
    mockCollection.findOne.mockResolvedValue(null);
    mockCollection.findOneAndUpdate.mockResolvedValue(null);
    mockCollection.updateOne.mockResolvedValue({ acknowledged: true });

    // Create instance
    bluestreak = new Bluestreak({
      dbUrl: "mongodb://localhost:27017",
      dbName: "test-db",
      shouldStop: () => false,
      timeoutInterval: 10000,
      pollInterval: 5000,
      waitRetryInterval: 1000,
    });
  });

  describe("Error Classes", () => {
    test("WorkflowNotFound error", () => {
      const error = new WorkflowNotFound("workflow-123");
      expect(error.name).toBe("WorkflowNotFound");
      expect(error.message).toBe("workflow not found: workflow-123");
      expect(error.workflowId).toBe("workflow-123");
      expect(error).toBeInstanceOf(Error);
    });

    test("HandlerNotFound error", () => {
      const error = new HandlerNotFound("handler-456");
      expect(error.name).toBe("HandlerNotFound");
      expect(error.message).toBe("handler not found: handler-456");
      expect(error.handlerId).toBe("handler-456");
      expect(error).toBeInstanceOf(Error);
    });

    test("WaitTimeout error", () => {
      const error = new WaitTimeout("workflow-789");
      expect(error.name).toBe("WaitTimeout");
      expect(error.message).toBe("wait timeout: workflow-789");
      expect(error.workflowId).toBe("workflow-789");
      expect(error).toBeInstanceOf(Error);
    });

    test("WorkflowAlreadyStarted error", () => {
      const error = new WorkflowAlreadyStarted("workflow-abc");
      expect(error.name).toBe("WorkflowAlreadyStarted");
      expect(error.message).toBe("workflow already started: workflow-abc");
      expect(error.workflowId).toBe("workflow-abc");
      expect(error).toBeInstanceOf(Error);
    });
  });

  describe("Initialization", () => {
    test("should initialize MongoDB connection", async () => {
      await bluestreak.init();

      expect(MockMongoClient).toHaveBeenCalledWith("mongodb://localhost:27017");
      expect(mockClient.db).toHaveBeenCalledWith("test-db");
      expect(mockDb.collection).toHaveBeenCalledWith("workflows");
      expect(mockCollection.createIndex).toHaveBeenCalledWith(
        { id: 1 },
        { unique: true }
      );
      expect(mockCollection.createIndex).toHaveBeenCalledWith({
        status: 1,
        timeoutAt: 1,
      });
    });

    test("should close MongoDB connection", async () => {
      await bluestreak.init();
      await bluestreak.close();

      expect(mockClient.close).toHaveBeenCalled();
    });
  });

  // Placeholder test to verify setup works
  test("placeholder test", () => {
    expect(true).toBe(true);
  });
});
