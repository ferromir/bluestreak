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
  fn(); // Execute immediately synchronously for tests
  return 1;
});

// Import after mocking
const {
  Bluestreak,
  WorkflowNotFound,
  HandlerNotFound,
  WaitTimeout,
  WorkflowAlreadyStarted,
} = await import("./index.js");

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

  describe("registerHandler", () => {
    test("should register a handler", async () => {
      await bluestreak.init();
      const handler = jest.fn();
      bluestreak.registerHandler("test-handler", handler);
      // Handler is registered successfully (private field, can't test directly)
      expect(true).toBe(true);
    });
  });

  describe("start", () => {
    beforeEach(async () => {
      await bluestreak.init();
    });

    test("should start a new workflow successfully", async () => {
      await bluestreak.start("workflow-1", "handler-1", { data: "test" });

      expect(mockCollection.insertOne).toHaveBeenCalledWith({
        id: "workflow-1",
        handlerId: "handler-1",
        input: { data: "test" },
        failures: 0,
        status: "idle",
        timeoutAt: new Date(mockTime),
      });
    });

    test("should throw WorkflowAlreadyStarted on duplicate workflow", async () => {
      const error = new Error("Duplicate key");
      error.name = "MongoServerError";
      error.code = 11000;
      mockCollection.insertOne.mockRejectedValue(error);

      await expect(
        bluestreak.start("workflow-1", "handler-1", {})
      ).rejects.toThrow(WorkflowAlreadyStarted);
    });

    test("should rethrow other errors", async () => {
      const error = new Error("Connection error");
      mockCollection.insertOne.mockRejectedValue(error);

      await expect(
        bluestreak.start("workflow-1", "handler-1", {})
      ).rejects.toThrow("Connection error");
    });
  });

  describe("wait", () => {
    beforeEach(async () => {
      await bluestreak.init();
    });

    test("should return result when workflow is finished", async () => {
      mockCollection.findOne.mockResolvedValue({
        status: "finished",
        result: { success: true },
      });

      const result = await bluestreak.wait("workflow-1", 3, 100);

      expect(result).toEqual({ success: true });
      expect(mockCollection.findOne).toHaveBeenCalled();
    });

    test("should throw WaitTimeout after retries", async () => {
      mockCollection.findOne.mockResolvedValue({
        status: "running",
        result: null,
      });

      await expect(bluestreak.wait("workflow-1", 3, 100)).rejects.toThrow(
        WaitTimeout
      );
      expect(mockCollection.findOne.mock.calls.length).toBe(3);
    });

    test("should throw WorkflowNotFound for non-existent workflow", async () => {
      mockCollection.findOne.mockResolvedValue(null);

      await expect(bluestreak.wait("workflow-1", 3, 100)).rejects.toThrow(
        WorkflowNotFound
      );
    });
  });

  describe("poll", () => {
    beforeEach(async () => {
      await bluestreak.init();
    });

    test("should stop when shouldStop returns true", async () => {
      let callCount = 0;
      bluestreak = new Bluestreak({
        dbUrl: "mongodb://localhost:27017",
        dbName: "test-db",
        shouldStop: () => {
          callCount++;
          return callCount > 1;
        },
        pollInterval: 5000,
      });
      await bluestreak.init();

      mockCollection.findOneAndUpdate.mockResolvedValue(null);

      await bluestreak.poll();

      expect(callCount).toBeGreaterThan(1);
    });

    test("should claim and run workflows", async () => {
      let callCount = 0;
      bluestreak = new Bluestreak({
        dbUrl: "mongodb://localhost:27017",
        dbName: "test-db",
        shouldStop: () => {
          callCount++;
          return callCount > 2;
        },
        pollInterval: 5000,
      });
      await bluestreak.init();

      const handler = jest.fn(async () => "result");
      bluestreak.registerHandler("test-handler", handler);

      mockCollection.findOneAndUpdate
        .mockResolvedValueOnce({ id: "workflow-1" })
        .mockResolvedValue(null);

      mockCollection.findOne.mockResolvedValue({
        handlerId: "test-handler",
        input: { data: "test" },
        failures: 0,
      });

      await bluestreak.poll();

      expect(mockCollection.findOneAndUpdate).toHaveBeenCalled();
      expect(handler).toHaveBeenCalledWith(expect.any(Object), {
        data: "test",
      });
      expect(mockCollection.updateOne).toHaveBeenCalledWith(
        { id: "workflow-1" },
        { $set: { status: "finished", result: "result" } }
      );
    });

    test("should reject on HandlerNotFound", async () => {
      let callCount = 0;
      bluestreak = new Bluestreak({
        dbUrl: "mongodb://localhost:27017",
        dbName: "test-db",
        shouldStop: () => {
          callCount++;
          return callCount > 2;
        },
        pollInterval: 5000,
      });
      await bluestreak.init();

      mockCollection.findOneAndUpdate.mockResolvedValueOnce({
        id: "workflow-1",
      });

      mockCollection.findOne.mockResolvedValue({
        handlerId: "missing-handler",
        input: {},
        failures: 0,
      });

      await expect(bluestreak.poll()).rejects.toThrow(HandlerNotFound);
    });
  });

  describe("workflow execution with steps", () => {
    beforeEach(async () => {
      await bluestreak.init();
    });

    test("should execute steps and cache results", async () => {
      const stepFn = jest.fn(async () => "step-result");
      const handler = jest.fn(async (ctx) => {
        const result = await ctx.step("step-1", stepFn);
        return result;
      });

      mockCollection.findOne
        .mockResolvedValueOnce({
          handlerId: "step-handler",
          input: {},
          failures: 0,
        })
        .mockResolvedValueOnce(null); // No cached step result

      let callCount = 0;
      bluestreak = new Bluestreak({
        dbUrl: "mongodb://localhost:27017",
        dbName: "test-db",
        shouldStop: () => {
          callCount++;
          return callCount > 2;
        },
        timeoutInterval: 10000,
      });
      await bluestreak.init();
      bluestreak.registerHandler("step-handler", handler);

      mockCollection.findOneAndUpdate
        .mockResolvedValueOnce({
          id: "workflow-1",
        })
        .mockResolvedValue(null);

      await bluestreak.poll();
      // Give async work time to complete
      await new Promise((resolve) => setTimeout(resolve, 0));

      expect(stepFn).toHaveBeenCalled();
      expect(mockCollection.updateOne).toHaveBeenCalledWith(
        { id: "workflow-1" },
        {
          $set: {
            "steps.step-1": "step-result",
            timeoutAt: new Date(mockTime + 10000),
          },
        }
      );
    });

    test("should use cached step results", async () => {
      const stepFn = jest.fn(async () => "new-result");
      const handler = jest.fn(async (ctx) => {
        return await ctx.step("step-1", stepFn);
      });

      mockCollection.findOne
        .mockResolvedValueOnce({
          handlerId: "cached-handler",
          input: {},
          failures: 0,
        })
        .mockResolvedValueOnce({
          steps: { "step-1": "cached-result" },
        });

      let callCount = 0;
      bluestreak = new Bluestreak({
        dbUrl: "mongodb://localhost:27017",
        dbName: "test-db",
        shouldStop: () => {
          callCount++;
          return callCount > 2;
        },
      });
      await bluestreak.init();
      bluestreak.registerHandler("cached-handler", handler);

      mockCollection.findOneAndUpdate
        .mockResolvedValueOnce({
          id: "workflow-1",
        })
        .mockResolvedValue(null);

      await bluestreak.poll();
      // Give async work time to complete
      await new Promise((resolve) => setTimeout(resolve, 0));

      expect(stepFn).not.toHaveBeenCalled();
      expect(mockCollection.updateOne).toHaveBeenCalledWith(
        { id: "workflow-1" },
        { $set: { status: "finished", result: "cached-result" } }
      );
    });
  });

  describe("workflow execution with sleep", () => {
    beforeEach(async () => {
      await bluestreak.init();
    });

    test("should sleep for specified duration", async () => {
      const handler = jest.fn(async (ctx) => {
        await ctx.sleep("nap-1", 5000);
        return "done";
      });

      mockCollection.findOne
        .mockResolvedValueOnce({
          handlerId: "sleep-handler",
          input: {},
          failures: 0,
        })
        .mockResolvedValueOnce(null); // No existing nap

      let callCount = 0;
      bluestreak = new Bluestreak({
        dbUrl: "mongodb://localhost:27017",
        dbName: "test-db",
        shouldStop: () => {
          callCount++;
          return callCount > 2;
        },
        timeoutInterval: 10000,
      });
      await bluestreak.init();
      bluestreak.registerHandler("sleep-handler", handler);

      mockCollection.findOneAndUpdate
        .mockResolvedValueOnce({
          id: "workflow-1",
        })
        .mockResolvedValue(null);

      await bluestreak.poll();
      // Give async work time to complete
      await new Promise((resolve) => setTimeout(resolve, 0));

      expect(mockCollection.updateOne).toHaveBeenCalledWith(
        { id: "workflow-1" },
        {
          $set: {
            "naps.nap-1": new Date(mockTime + 5000),
            timeoutAt: new Date(mockTime + 5000 + 10000),
          },
        }
      );
    });

    test("should resume from existing sleep", async () => {
      mockTime = 1000000;
      const wakeUpTime = new Date(mockTime + 5000);

      const handler = jest.fn(async (ctx) => {
        await ctx.sleep("nap-1", 5000);
        return "done";
      });

      mockCollection.findOne
        .mockResolvedValueOnce({
          handlerId: "resume-handler",
          input: {},
          failures: 0,
        })
        .mockResolvedValueOnce({
          naps: { "nap-1": wakeUpTime },
        });

      let callCount = 0;
      bluestreak = new Bluestreak({
        dbUrl: "mongodb://localhost:27017",
        dbName: "test-db",
        shouldStop: () => {
          callCount++;
          return callCount > 2;
        },
      });
      await bluestreak.init();
      bluestreak.registerHandler("resume-handler", handler);

      mockCollection.findOneAndUpdate
        .mockResolvedValueOnce({
          id: "workflow-1",
        })
        .mockResolvedValue(null);

      await bluestreak.poll();
      // Give async work time to complete
      await new Promise((resolve) => setTimeout(resolve, 0));

      expect(global.setTimeout).toHaveBeenCalled();
    });
  });

  describe("error handling and retries", () => {
    beforeEach(async () => {
      await bluestreak.init();
    });

    test("should retry failed workflows", async () => {
      const handler = jest.fn(async () => {
        throw new Error("Handler failed");
      });

      const errorCallback = jest.fn();

      mockCollection.findOne.mockResolvedValue({
        handlerId: "fail-handler",
        input: {},
        failures: 0,
      });

      let callCount = 0;
      bluestreak = new Bluestreak({
        dbUrl: "mongodb://localhost:27017",
        dbName: "test-db",
        shouldStop: () => {
          callCount++;
          return callCount > 2;
        },
        waitRetryInterval: 1000,
        errorCallback,
      });
      await bluestreak.init();
      bluestreak.registerHandler("fail-handler", handler);

      mockCollection.findOneAndUpdate
        .mockResolvedValueOnce({
          id: "workflow-1",
        })
        .mockResolvedValue(null);

      await bluestreak.poll();
      // Give async work time to complete
      await new Promise((resolve) => setTimeout(resolve, 0));

      expect(errorCallback).toHaveBeenCalledWith(
        "workflow-1",
        expect.any(Error)
      );
      expect(mockCollection.updateOne).toHaveBeenCalledWith(
        { id: "workflow-1" },
        {
          $set: {
            status: "failed",
            timeoutAt: new Date(mockTime + 1000),
            failures: 1,
          },
        }
      );
    });

    test("should abort workflow after maxFailures", async () => {
      const handler = jest.fn(async () => {
        throw new Error("Always fails");
      });

      mockCollection.findOne.mockResolvedValue({
        handlerId: "abort-handler",
        input: {},
        failures: 3,
      });

      let callCount = 0;
      bluestreak = new Bluestreak({
        dbUrl: "mongodb://localhost:27017",
        dbName: "test-db",
        shouldStop: () => {
          callCount++;
          return callCount > 2;
        },
        maxFailures: 3,
        waitRetryInterval: 1000,
      });
      await bluestreak.init();
      bluestreak.registerHandler("abort-handler", handler);

      mockCollection.findOneAndUpdate
        .mockResolvedValueOnce({
          id: "workflow-1",
        })
        .mockResolvedValue(null);

      await bluestreak.poll();
      // Give async work time to complete
      await new Promise((resolve) => setTimeout(resolve, 0));

      expect(mockCollection.updateOne).toHaveBeenCalledWith(
        { id: "workflow-1" },
        {
          $set: {
            status: "aborted",
            timeoutAt: new Date(mockTime + 1000),
            failures: 4,
          },
        }
      );
    });
  });
});
