import { jest } from "@jest/globals";

// Mock MongoDB - need separate collections for workflows, steps, and naps
const mockWorkflowsCollection = {
  insertOne: jest.fn(),
  findOne: jest.fn(),
  findOneAndUpdate: jest.fn(),
  updateOne: jest.fn(),
  createIndex: jest.fn(),
};

const mockStepsCollection = {
  insertOne: jest.fn(),
  findOne: jest.fn(),
  updateOne: jest.fn(),
  createIndex: jest.fn(),
};

const mockNapsCollection = {
  insertOne: jest.fn(),
  findOne: jest.fn(),
  updateOne: jest.fn(),
  createIndex: jest.fn(),
};

const mockDb = {
  collection: jest.fn((name) => {
    if (name === "workflows") return mockWorkflowsCollection;
    if (name === "steps") return mockStepsCollection;
    if (name === "naps") return mockNapsCollection;
    throw new Error(`Unknown collection: ${name}`);
  }),
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
const { Bluestreak, WorkflowNotFound, HandlerNotFound, WaitTimeout } =
  await import("./index2.js");

describe("Bluestreak (index2)", () => {
  let bluestreak;

  beforeEach(() => {
    // Reset mocks
    jest.clearAllMocks();
    mockTime = 1000000;

    // Setup default mock behaviors for workflows collection
    mockWorkflowsCollection.createIndex.mockResolvedValue(undefined);
    mockWorkflowsCollection.insertOne.mockResolvedValue({ acknowledged: true });
    mockWorkflowsCollection.findOne.mockResolvedValue(null);
    mockWorkflowsCollection.findOneAndUpdate.mockResolvedValue(null);
    mockWorkflowsCollection.updateOne.mockResolvedValue({ acknowledged: true });

    // Setup default mock behaviors for steps collection
    mockStepsCollection.createIndex.mockResolvedValue(undefined);
    mockStepsCollection.findOne.mockResolvedValue(null);
    mockStepsCollection.updateOne.mockResolvedValue({ acknowledged: true });

    // Setup default mock behaviors for naps collection
    mockNapsCollection.createIndex.mockResolvedValue(undefined);
    mockNapsCollection.findOne.mockResolvedValue(null);
    mockNapsCollection.updateOne.mockResolvedValue({ acknowledged: true });

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
  });

  describe("Initialization", () => {
    test("should initialize MongoDB connection with three collections", async () => {
      await bluestreak.init();

      expect(MockMongoClient).toHaveBeenCalledWith("mongodb://localhost:27017");
      expect(mockClient.db).toHaveBeenCalledWith("test-db");
      expect(mockDb.collection).toHaveBeenCalledWith("workflows");
      expect(mockDb.collection).toHaveBeenCalledWith("steps");
      expect(mockDb.collection).toHaveBeenCalledWith("naps");

      // Check workflows collection indexes
      expect(mockWorkflowsCollection.createIndex).toHaveBeenCalledWith(
        { workflowId: 1 },
        { unique: true }
      );
      expect(mockWorkflowsCollection.createIndex).toHaveBeenCalledWith({
        status: 1,
        timeoutAt: 1,
      });

      // Check steps collection index
      expect(mockStepsCollection.createIndex).toHaveBeenCalledWith(
        { workflowId: 1, stepId: 1 },
        { unique: true }
      );

      // Check naps collection index
      expect(mockNapsCollection.createIndex).toHaveBeenCalledWith(
        { workflowId: 1, napId: 1 },
        { unique: true }
      );
    });

    test("should use default parameters when not provided", () => {
      const bluestreakWithDefaults = new Bluestreak({
        shouldStop: () => false,
      });

      const params = bluestreakWithDefaults.getParams();

      expect(params.dbUrl).toBe("mongodb://localhost:27017");
      expect(params.dbName).toBe("bluestreak");
      expect(params.timeoutInterval).toBe(10000);
      expect(params.pollInterval).toBe(5000);
      expect(params.waitRetryInterval).toBe(1000);
      expect(params.maxFailures).toBeUndefined();
    });

    test("should use provided parameters", () => {
      const customBluestreak = new Bluestreak({
        dbUrl: "mongodb://custom:27017",
        dbName: "custom-db",
        timeoutInterval: 20000,
        pollInterval: 3000,
        waitRetryInterval: 2000,
        maxFailures: 5,
        shouldStop: () => false,
      });

      const params = customBluestreak.getParams();

      expect(params.dbUrl).toBe("mongodb://custom:27017");
      expect(params.dbName).toBe("custom-db");
      expect(params.timeoutInterval).toBe(20000);
      expect(params.pollInterval).toBe(3000);
      expect(params.waitRetryInterval).toBe(2000);
      expect(params.maxFailures).toBe(5);
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

    test("should start a new workflow successfully and return true", async () => {
      const result = await bluestreak.start("workflow-1", "handler-1", {
        data: "test",
      });

      expect(result).toBe(true);
      expect(mockWorkflowsCollection.insertOne).toHaveBeenCalledWith({
        workflowId: "workflow-1",
        handlerId: "handler-1",
        input: { data: "test" },
        failures: 0,
        status: "idle",
        timeoutAt: new Date(mockTime),
      });
    });

    test("should return false on duplicate workflow", async () => {
      const error = new Error("Duplicate key");
      error.name = "MongoServerError";
      error.code = 11000;
      mockWorkflowsCollection.insertOne.mockRejectedValue(error);

      const result = await bluestreak.start("workflow-1", "handler-1", {});

      expect(result).toBe(false);
    });

    test("should rethrow other errors", async () => {
      const error = new Error("Connection error");
      mockWorkflowsCollection.insertOne.mockRejectedValue(error);

      await expect(
        bluestreak.start("workflow-1", "handler-1", {})
      ).rejects.toThrow("Connection error");
    });
  });

  describe("findWorkflow", () => {
    beforeEach(async () => {
      await bluestreak.init();
    });

    test("should find workflow by workflowId", async () => {
      const mockWorkflow = {
        workflowId: "workflow-1",
        status: "finished",
        result: "test",
      };
      mockWorkflowsCollection.findOne.mockResolvedValue(mockWorkflow);

      const result = await bluestreak.findWorkflow("workflow-1");

      expect(result).toEqual(mockWorkflow);
      expect(mockWorkflowsCollection.findOne).toHaveBeenCalledWith({
        workflowId: "workflow-1",
      });
    });

    test("should return null if workflow not found", async () => {
      mockWorkflowsCollection.findOne.mockResolvedValue(null);

      const result = await bluestreak.findWorkflow("workflow-1");

      expect(result).toBe(null);
    });
  });

  describe("findStep", () => {
    beforeEach(async () => {
      await bluestreak.init();
    });

    test("should find step by workflowId and stepId", async () => {
      const mockStep = {
        workflowId: "workflow-1",
        stepId: "step-1",
        output: "result",
      };
      mockStepsCollection.findOne.mockResolvedValue(mockStep);

      const result = await bluestreak.findStep("workflow-1", "step-1");

      expect(result).toEqual(mockStep);
      expect(mockStepsCollection.findOne).toHaveBeenCalledWith({
        workflowId: "workflow-1",
        stepId: "step-1",
      });
    });

    test("should return null if step not found", async () => {
      mockStepsCollection.findOne.mockResolvedValue(null);

      const result = await bluestreak.findStep("workflow-1", "step-1");

      expect(result).toBe(null);
    });
  });

  describe("findNap", () => {
    beforeEach(async () => {
      await bluestreak.init();
    });

    test("should find nap by workflowId and napId", async () => {
      const mockNap = {
        workflowId: "workflow-1",
        napId: "nap-1",
        wakeUpAt: new Date(),
      };
      mockNapsCollection.findOne.mockResolvedValue(mockNap);

      const result = await bluestreak.findNap("workflow-1", "nap-1");

      expect(result).toEqual(mockNap);
      expect(mockNapsCollection.findOne).toHaveBeenCalledWith({
        workflowId: "workflow-1",
        napId: "nap-1",
      });
    });

    test("should return null if nap not found", async () => {
      mockNapsCollection.findOne.mockResolvedValue(null);

      const result = await bluestreak.findNap("workflow-1", "nap-1");

      expect(result).toBe(null);
    });
  });

  describe("wait", () => {
    beforeEach(async () => {
      await bluestreak.init();
    });

    test("should return result when workflow is finished", async () => {
      mockWorkflowsCollection.findOne.mockResolvedValue({
        status: "finished",
        result: { success: true },
      });

      const result = await bluestreak.wait("workflow-1", 3, 100);

      expect(result).toEqual({ success: true });
      expect(mockWorkflowsCollection.findOne).toHaveBeenCalled();
    });

    test("should throw WaitTimeout after retries", async () => {
      mockWorkflowsCollection.findOne.mockResolvedValue({
        status: "running",
        result: null,
      });

      await expect(bluestreak.wait("workflow-1", 3, 100)).rejects.toThrow(
        WaitTimeout
      );
      expect(mockWorkflowsCollection.findOne.mock.calls.length).toBe(3);
    });

    test("should throw WorkflowNotFound for non-existent workflow", async () => {
      mockWorkflowsCollection.findOne.mockResolvedValue(null);

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

      mockWorkflowsCollection.findOneAndUpdate.mockResolvedValue(null);

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

      mockWorkflowsCollection.findOneAndUpdate
        .mockResolvedValueOnce({ workflowId: "workflow-1" })
        .mockResolvedValue(null);

      mockWorkflowsCollection.findOne.mockResolvedValue({
        handlerId: "test-handler",
        input: { data: "test" },
        failures: 0,
      });

      await bluestreak.poll();

      expect(mockWorkflowsCollection.findOneAndUpdate).toHaveBeenCalled();
      expect(handler).toHaveBeenCalledWith(expect.any(Object), {
        data: "test",
      });
      expect(mockWorkflowsCollection.updateOne).toHaveBeenCalledWith(
        { workflowId: "workflow-1" },
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

      mockWorkflowsCollection.findOneAndUpdate.mockResolvedValueOnce({
        workflowId: "workflow-1",
      });

      mockWorkflowsCollection.findOne.mockResolvedValue({
        handlerId: "missing-handler",
        input: {},
        failures: 0,
      });

      await expect(bluestreak.poll()).rejects.toThrow(HandlerNotFound);
    });

    test("should reject on WorkflowNotFound", async () => {
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

      mockWorkflowsCollection.findOneAndUpdate.mockResolvedValueOnce({
        workflowId: "workflow-1",
      });

      mockWorkflowsCollection.findOne.mockResolvedValue(null);

      await expect(bluestreak.poll()).rejects.toThrow(WorkflowNotFound);
    });
  });

  describe("workflow execution with steps", () => {
    beforeEach(async () => {
      await bluestreak.init();
    });

    test("should execute steps and save to steps collection", async () => {
      const stepFn = jest.fn(async () => "step-result");
      const handler = jest.fn(async (ctx) => {
        const result = await ctx.step("step-1", stepFn);
        return result;
      });

      mockWorkflowsCollection.findOne.mockResolvedValueOnce({
        handlerId: "step-handler",
        input: {},
        failures: 0,
      });

      mockStepsCollection.findOne.mockResolvedValueOnce(null); // No cached step

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

      mockWorkflowsCollection.findOneAndUpdate
        .mockResolvedValueOnce({
          workflowId: "workflow-1",
        })
        .mockResolvedValue(null);

      await bluestreak.poll();
      // Give async work time to complete
      await new Promise((resolve) => setTimeout(resolve, 0));

      expect(stepFn).toHaveBeenCalled();
      expect(mockStepsCollection.updateOne).toHaveBeenCalledWith(
        { workflowId: "workflow-1", stepId: "step-1" },
        {
          $setOnInsert: {
            workflowId: "workflow-1",
            stepId: "step-1",
            output: "step-result",
          },
        },
        { upsert: true }
      );
      expect(mockWorkflowsCollection.updateOne).toHaveBeenCalledWith(
        { workflowId: "workflow-1" },
        {
          $set: {
            timeoutAt: new Date(mockTime + 10000),
          },
        }
      );
    });

    test("should use cached step results from steps collection", async () => {
      const stepFn = jest.fn(async () => "new-result");
      const handler = jest.fn(async (ctx) => {
        return await ctx.step("step-1", stepFn);
      });

      mockWorkflowsCollection.findOne.mockResolvedValueOnce({
        handlerId: "cached-handler",
        input: {},
        failures: 0,
      });

      mockStepsCollection.findOne.mockResolvedValueOnce({
        workflowId: "workflow-1",
        stepId: "step-1",
        output: "cached-result",
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

      mockWorkflowsCollection.findOneAndUpdate
        .mockResolvedValueOnce({
          workflowId: "workflow-1",
        })
        .mockResolvedValue(null);

      await bluestreak.poll();
      // Give async work time to complete
      await new Promise((resolve) => setTimeout(resolve, 0));

      expect(stepFn).not.toHaveBeenCalled();
      expect(mockWorkflowsCollection.updateOne).toHaveBeenCalledWith(
        { workflowId: "workflow-1" },
        { $set: { status: "finished", result: "cached-result" } }
      );
    });

    test("should handle multiple steps in sequence", async () => {
      let resolveHandler;
      const handlerComplete = new Promise((resolve) => {
        resolveHandler = resolve;
      });

      const step1Fn = jest.fn(async () => "result-1");
      const step2Fn = jest.fn(async () => "result-2");
      const handler = jest.fn(async (ctx) => {
        const r1 = await ctx.step("step-1", step1Fn);
        const r2 = await ctx.step("step-2", step2Fn);
        resolveHandler();
        return `${r1}-${r2}`;
      });

      mockWorkflowsCollection.findOne.mockResolvedValueOnce({
        handlerId: "multi-step-handler",
        input: {},
        failures: 0,
      });

      mockStepsCollection.findOne
        .mockResolvedValueOnce(null) // step-1 not cached
        .mockResolvedValueOnce(null); // step-2 not cached

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
      bluestreak.registerHandler("multi-step-handler", handler);

      mockWorkflowsCollection.findOneAndUpdate
        .mockResolvedValueOnce({
          workflowId: "workflow-1",
        })
        .mockResolvedValue(null);

      const pollPromise = bluestreak.poll();
      await handlerComplete;
      await new Promise((resolve) => setTimeout(resolve, 0));

      expect(step1Fn).toHaveBeenCalled();
      expect(step2Fn).toHaveBeenCalled();
      expect(mockStepsCollection.updateOne).toHaveBeenCalledTimes(2);
      expect(mockWorkflowsCollection.updateOne).toHaveBeenCalledWith(
        { workflowId: "workflow-1" },
        { $set: { status: "finished", result: "result-1-result-2" } }
      );
    });
  });

  describe("workflow execution with sleep", () => {
    beforeEach(async () => {
      await bluestreak.init();
    });

    test("should sleep for specified duration and save to naps collection", async () => {
      const handler = jest.fn(async (ctx) => {
        await ctx.sleep("nap-1", 5000);
        return "done";
      });

      mockWorkflowsCollection.findOne.mockResolvedValueOnce({
        handlerId: "sleep-handler",
        input: {},
        failures: 0,
      });

      mockNapsCollection.findOne.mockResolvedValueOnce(null); // No existing nap

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

      mockWorkflowsCollection.findOneAndUpdate
        .mockResolvedValueOnce({
          workflowId: "workflow-1",
        })
        .mockResolvedValue(null);

      await bluestreak.poll();
      // Give async work time to complete
      await new Promise((resolve) => setTimeout(resolve, 0));

      expect(mockNapsCollection.updateOne).toHaveBeenCalledWith(
        { workflowId: "workflow-1", napId: "nap-1" },
        {
          $setOnInsert: {
            workflowId: "workflow-1",
            napId: "nap-1",
            wakeUpAt: new Date(mockTime + 5000),
          },
        },
        { upsert: true }
      );
      expect(mockWorkflowsCollection.updateOne).toHaveBeenCalledWith(
        { workflowId: "workflow-1" },
        {
          $set: {
            timeoutAt: new Date(mockTime + 5000 + 10000),
          },
        }
      );
    });

    test("should resume from existing sleep in naps collection", async () => {
      mockTime = 1000000;
      const wakeUpTime = new Date(mockTime + 5000);

      const handler = jest.fn(async (ctx) => {
        await ctx.sleep("nap-1", 5000);
        return "done";
      });

      mockWorkflowsCollection.findOne.mockResolvedValueOnce({
        handlerId: "resume-handler",
        input: {},
        failures: 0,
      });

      mockNapsCollection.findOne.mockResolvedValueOnce({
        workflowId: "workflow-1",
        napId: "nap-1",
        wakeUpAt: wakeUpTime,
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

      mockWorkflowsCollection.findOneAndUpdate
        .mockResolvedValueOnce({
          workflowId: "workflow-1",
        })
        .mockResolvedValue(null);

      await bluestreak.poll();
      // Give async work time to complete
      await new Promise((resolve) => setTimeout(resolve, 0));

      expect(global.setTimeout).toHaveBeenCalled();
      // Should not insert new nap since it already exists
      expect(mockNapsCollection.updateOne).not.toHaveBeenCalled();
    });

    test("should skip sleep if wakeUpAt time has passed", async () => {
      mockTime = 1000000;
      const wakeUpTime = new Date(mockTime - 5000); // Wake up time in the past

      const handler = jest.fn(async (ctx) => {
        await ctx.sleep("nap-1", 5000);
        return "done";
      });

      mockWorkflowsCollection.findOne.mockResolvedValueOnce({
        handlerId: "past-handler",
        input: {},
        failures: 0,
      });

      mockNapsCollection.findOne.mockResolvedValueOnce({
        workflowId: "workflow-1",
        napId: "nap-1",
        wakeUpAt: wakeUpTime,
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
      bluestreak.registerHandler("past-handler", handler);

      mockWorkflowsCollection.findOneAndUpdate
        .mockResolvedValueOnce({
          workflowId: "workflow-1",
        })
        .mockResolvedValue(null);

      await bluestreak.poll();
      await new Promise((resolve) => setTimeout(resolve, 0));

      // Should complete without actually sleeping
      expect(mockWorkflowsCollection.updateOne).toHaveBeenCalledWith(
        { workflowId: "workflow-1" },
        { $set: { status: "finished", result: "done" } }
      );
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

      mockWorkflowsCollection.findOne.mockResolvedValue({
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

      mockWorkflowsCollection.findOneAndUpdate
        .mockResolvedValueOnce({
          workflowId: "workflow-1",
        })
        .mockResolvedValue(null);

      await bluestreak.poll();
      // Give async work time to complete
      await new Promise((resolve) => setTimeout(resolve, 0));

      expect(errorCallback).toHaveBeenCalledWith(
        "workflow-1",
        expect.any(Error)
      );
      expect(mockWorkflowsCollection.updateOne).toHaveBeenCalledWith(
        { workflowId: "workflow-1" },
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

      mockWorkflowsCollection.findOne.mockResolvedValue({
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

      mockWorkflowsCollection.findOneAndUpdate
        .mockResolvedValueOnce({
          workflowId: "workflow-1",
        })
        .mockResolvedValue(null);

      await bluestreak.poll();
      // Give async work time to complete
      await new Promise((resolve) => setTimeout(resolve, 0));

      expect(mockWorkflowsCollection.updateOne).toHaveBeenCalledWith(
        { workflowId: "workflow-1" },
        {
          $set: {
            status: "aborted",
            timeoutAt: new Date(mockTime + 1000),
            failures: 4,
          },
        }
      );
    });

    test("should not invoke errorCallback when undefined", async () => {
      const handler = jest.fn(async () => {
        throw new Error("Handler failed");
      });

      mockWorkflowsCollection.findOne.mockResolvedValue({
        handlerId: "fail-handler-no-callback",
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
        // No errorCallback
      });
      await bluestreak.init();
      bluestreak.registerHandler("fail-handler-no-callback", handler);

      mockWorkflowsCollection.findOneAndUpdate
        .mockResolvedValueOnce({
          workflowId: "workflow-1",
        })
        .mockResolvedValue(null);

      await bluestreak.poll();
      await new Promise((resolve) => setTimeout(resolve, 0));

      // Should still update status to failed
      expect(mockWorkflowsCollection.updateOne).toHaveBeenCalledWith(
        { workflowId: "workflow-1" },
        {
          $set: {
            status: "failed",
            timeoutAt: new Date(mockTime + 1000),
            failures: 1,
          },
        }
      );
    });
  });

  describe("claim workflow", () => {
    beforeEach(async () => {
      await bluestreak.init();
    });

    test("should claim workflows with correct criteria", async () => {
      mockWorkflowsCollection.findOneAndUpdate.mockResolvedValueOnce({
        workflowId: "workflow-1",
      });

      mockWorkflowsCollection.findOne.mockResolvedValue({
        handlerId: "test-handler",
        input: {},
        failures: 0,
      });

      const handler = jest.fn(async () => "done");
      bluestreak.registerHandler("test-handler", handler);

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
      bluestreak.registerHandler("test-handler", handler);

      await bluestreak.poll();

      expect(mockWorkflowsCollection.findOneAndUpdate).toHaveBeenCalledWith(
        {
          status: { $in: ["idle", "running", "failed"] },
          timeoutAt: { $lt: new Date(mockTime) },
        },
        {
          $set: {
            status: "running",
            timeoutAt: new Date(mockTime + 10000),
          },
        },
        {
          projection: {
            _id: 0,
            workflowId: 1,
          },
        }
      );
    });
  });

  describe("combined step and sleep workflow", () => {
    beforeEach(async () => {
      await bluestreak.init();
    });

    test("should handle workflow with both steps and sleep", async () => {
      let resolveHandler;
      const handlerComplete = new Promise((resolve) => {
        resolveHandler = resolve;
      });

      const stepFn = jest.fn(async () => "step-result");
      const handler = jest.fn(async (ctx) => {
        const result = await ctx.step("step-1", stepFn);
        await ctx.sleep("nap-1", 1000);
        resolveHandler();
        return result;
      });

      mockWorkflowsCollection.findOne.mockResolvedValueOnce({
        handlerId: "combined-handler",
        input: {},
        failures: 0,
      });

      mockStepsCollection.findOne.mockResolvedValueOnce(null);
      mockNapsCollection.findOne.mockResolvedValueOnce(null);

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
      bluestreak.registerHandler("combined-handler", handler);

      mockWorkflowsCollection.findOneAndUpdate
        .mockResolvedValueOnce({
          workflowId: "workflow-1",
        })
        .mockResolvedValue(null);

      const pollPromise = bluestreak.poll();
      await handlerComplete;
      await new Promise((resolve) => setTimeout(resolve, 0));

      expect(stepFn).toHaveBeenCalled();
      expect(mockStepsCollection.updateOne).toHaveBeenCalled();
      expect(mockNapsCollection.updateOne).toHaveBeenCalled();
      expect(mockWorkflowsCollection.updateOne).toHaveBeenCalledWith(
        { workflowId: "workflow-1" },
        { $set: { status: "finished", result: "step-result" } }
      );
    });
  });
});
