import { MongoClient } from "mongodb";

/**
 * Error thrown when a workflow is not found in the database.
 */
export class WorkflowNotFound extends Error {
  /**
   * @param {string} workflowId - The ID of the workflow that was not found
   */
  constructor(workflowId) {
    super(`workflow not found: ${workflowId}`);
    this.name = "WorkflowNotFound";
    this.workflowId = workflowId;
  }
}

/**
 * Error thrown when a handler is not registered.
 */
export class HandlerNotFound extends Error {
  /**
   * @param {string} handlerId - The ID of the handler that was not found
   */
  constructor(handlerId) {
    super(`handler not found: ${handlerId}`);
    this.name = "HandlerNotFound";
    this.handlerId = handlerId;
  }
}

/**
 * Error thrown when waiting for a workflow to complete times out.
 */
export class WaitTimeout extends Error {
  /**
   * @param {string} workflowId - The ID of the workflow that timed out
   */
  constructor(workflowId) {
    super(`wait timeout: ${workflowId}`);
    this.name = "WaitTimeout";
    this.workflowId = workflowId;
  }
}

/**
 * Error thrown when attempting to start a workflow that already exists.
 */
export class WorkflowAlreadyStarted extends Error {
  /**
   * @param {string} workflowId - The ID of the workflow that already exists
   */
  constructor(workflowId) {
    super(`workflow already started: ${workflowId}`);
    this.name = "WorkflowAlreadyStarted";
    this.workflowId = workflowId;
  }
}

/**
 * @typedef {Object} WorkflowContext
 * @property {(stepId: string, fn: () => Promise<any>) => Promise<any>} step - Execute an idempotent step
 * @property {(napId: string, ms: number) => Promise<void>} sleep - Sleep for a duration
 */

/**
 * @callback WorkflowHandler
 * @param {WorkflowContext} ctx - The workflow context with step and sleep methods
 * @param {any} input - The input data passed to the workflow
 * @returns {Promise<any>} The result of the workflow execution
 */

/**
 * @callback ErrorCallback
 * @param {string} workflowId - The ID of the workflow that encountered an error
 * @param {Error} error - The error that occurred
 */

/**
 * @callback ShouldStopCallback
 * @returns {boolean} Whether the poll loop should stop
 */

/**
 * @typedef {Object} BluestreakParams
 * @property {string} [dbUrl="mongodb://localhost:27017"] - MongoDB connection URL
 * @property {string} [dbName="bluestreak"] - MongoDB database name
 * @property {number} [timeoutInterval=10000] - Timeout interval in milliseconds for workflow execution
 * @property {number} [pollInterval=5000] - Interval in milliseconds between poll attempts when no workflows are available
 * @property {number} [waitRetryInterval=1000] - Interval in milliseconds before retrying a failed workflow
 * @property {ErrorCallback} [errorCallback] - Callback invoked when a workflow handler throws an error
 * @property {number} [maxFailures] - Maximum number of failures before aborting a workflow
 * @property {ShouldStopCallback} [shouldStop] - Callback to determine when to stop polling
 */

/**
 * Bluestreak - A lightweight durable execution library.
 *
 * Provides durable workflow execution with automatic retries, idempotent steps,
 * and persistent state storage using MongoDB.
 */
export class Bluestreak {
  #dbUrl;
  #dbName;
  #client;
  #workflows;
  #timeoutInterval;
  #pollInterval;
  #waitRetryInterval;
  #errorCallback;
  #maxFailures;
  #shouldStop;
  #handlers;

  /**
   * Creates a new Bluestreak instance.
   *
   * @param {BluestreakParams} params - Configuration parameters
   */
  constructor(params) {
    this.#dbUrl = params.dbUrl || "mongodb://localhost:27017";
    this.#dbName = params.dbName || "bluestreak";
    this.#client = null;
    this.#workflows = null;
    this.#timeoutInterval = params.timeoutInterval || 10_000;
    this.#pollInterval = params.pollInterval || 5_000;
    this.#waitRetryInterval = params.waitRetryInterval || 1_000;
    this.#errorCallback = params.errorCallback;
    this.#maxFailures = params.maxFailures;
    this.#shouldStop = params.shouldStop;
    this.#handlers = new Map();
  }

  /**
   * Registers a workflow handler that can be invoked by workflow executions.
   *
   * @param {string} handlerId - Unique identifier for the handler
   * @param {WorkflowHandler} handler - The handler function to execute workflows
   */
  registerHandler(handlerId, handler) {
    this.#handlers.set(handlerId, handler);
  }

  /**
   * Initializes the MongoDB connection and creates required indexes.
   *
   * @returns {Promise<void>}
   */
  async init() {
    this.#client = new MongoClient(this.#dbUrl);
    const db = this.#client.db(this.#dbName);
    this.#workflows = db.collection("workflows");
    await this.#workflows.createIndex({ id: 1 }, { unique: true });
    await this.#workflows.createIndex({ status: 1, timeoutAt: 1 });
  }

  /**
   * Closes the MongoDB connection.
   *
   * @returns {Promise<void>}
   */
  async close() {
    await this.#client.close();
  }

  /**
   * Starts a new workflow execution.
   *
   * @param {string} workflowId - Unique identifier for the workflow
   * @param {string} handlerId - The ID of the handler to execute
   * @param {any} input - Input data to pass to the workflow handler
   * @returns {Promise<void>}
   * @throws {WorkflowAlreadyStarted} If a workflow with the same ID already exists
   */
  async start(workflowId, handlerId, input) {
    try {
      await this.#insert(workflowId, handlerId, input);
    } catch (err) {
      if (err.name === "MongoServerError" && err.code === 11000) {
        throw new WorkflowAlreadyStarted(workflowId);
      }
      throw err;
    }
  }

  /**
   * Waits for a workflow to complete by polling its status.
   *
   * @param {string} workflowId - The ID of the workflow to wait for
   * @param {number} retries - Number of times to check the workflow status
   * @param {number} pauseInterval - Milliseconds to wait between retries
   * @returns {Promise<any>} The result of the workflow execution
   * @throws {WaitTimeout} If the workflow doesn't complete within the retry limit
   * @throws {WorkflowNotFound} If the workflow doesn't exist
   */
  async wait(workflowId, retries, pauseInterval) {
    for (let i = 0; i < retries; i++) {
      const data = await this.#findStatusAndResult(workflowId);
      if (data.status === "finished") {
        return data.result;
      }
      await this.#goSleep(pauseInterval);
    }
    throw new WaitTimeout(workflowId);
  }

  /**
   * Starts the workflow execution loop that claims and processes workflows.
   *
   * The loop runs until the shouldStop callback returns true. Workflows are
   * executed in a fire-and-forget pattern. Handler errors trigger retries,
   * while infrastructure errors (HandlerNotFound, WorkflowNotFound) will
   * reject the promise and stop the loop.
   *
   * @returns {Promise<void>}
   * @throws {HandlerNotFound} If a workflow references a non-existent handler
   * @throws {WorkflowNotFound} If a claimed workflow is not found
   */
  async poll() {
    return new Promise(async (resolve, reject) => {
      let hasRejected = false;
      while (!this.#shouldStop()) {
        const workflowId = await this.#claim();
        if (workflowId) {
          this.#run(workflowId).catch((err) => {
            if (!hasRejected) {
              hasRejected = true;
              reject(err);
            }
          });
        } else {
          await this.#goSleep(this.#pollInterval);
        }
      }
      resolve();
    });
  }

  async #run(workflowId) {
    const runData = await this.#findRunData(workflowId);
    if (!runData) {
      throw new WorkflowNotFound(workflowId);
    }
    const handler = this.#handlers.get(runData.handlerId);
    if (!handler) {
      throw new HandlerNotFound(runData.handlerId);
    }
    const ctx = {
      step: this.#step(workflowId).bind(this),
      sleep: this.#sleep(workflowId).bind(this),
    };
    let result;
    try {
      result = await handler(ctx, runData.input);
    } catch (err) {
      const failures = runData.failures + 1;
      let status = "failed";
      if (this.#maxFailures !== undefined && failures > this.#maxFailures) {
        status = "aborted";
      }
      const now = new Date();
      const timeoutAt = new Date(now.getTime() + this.#waitRetryInterval);
      await this.#updateStatus(workflowId, status, timeoutAt, failures);
      if (this.#errorCallback) {
        this.#errorCallback(workflowId, err);
      }
      return;
    }
    await this.#setAsFinished(workflowId, result);
  }

  #step(workflowId) {
    return async function (stepId, fn) {
      let output = await this.#findOutput(workflowId, stepId);
      if (output !== undefined) {
        return output;
      }
      output = await fn();
      const now = new Date();
      const timeoutAt = new Date(now.getTime() + this.#timeoutInterval);
      await this.#updateOutput(workflowId, stepId, output, timeoutAt);
      return output;
    };
  }

  #sleep(workflowId) {
    return async function (napId, ms) {
      let wakeUpAt = await this.#findWakeUpAt(workflowId, napId);
      const now = new Date();
      if (wakeUpAt) {
        const remainingMs = wakeUpAt.getTime() - now.getTime();
        if (remainingMs > 0) {
          await this.#goSleep(remainingMs);
        }
        return;
      }
      wakeUpAt = new Date(now.getTime() + ms);
      const timeoutAt = new Date(wakeUpAt.getTime() + this.#timeoutInterval);
      await this.#updateWakeUpAt(workflowId, napId, wakeUpAt, timeoutAt);
      await this.#goSleep(ms);
    };
  }

  async #insert(workflowId, handlerId, input) {
    const now = new Date();
    await this.#workflows.insertOne({
      id: workflowId,
      handlerId: handlerId,
      input,
      failures: 0,
      status: "idle",
      timeoutAt: now,
    });
  }

  async #findOutput(workflowId, stepId) {
    const workflow = await this.#workflows.findOne(
      {
        id: workflowId,
      },
      {
        projection: {
          _id: 0,
          [`steps.${stepId}`]: 1,
        },
      }
    );
    if (workflow && workflow.steps) {
      return workflow.steps[stepId];
    }
    return undefined;
  }

  async #findWakeUpAt(workflowId, napId) {
    const workflow = await this.#workflows.findOne(
      {
        id: workflowId,
      },
      {
        projection: {
          _id: 0,
          [`naps.${napId}`]: 1,
        },
      }
    );
    if (workflow && workflow.naps) {
      return workflow.naps[napId];
    }
    return undefined;
  }

  async #findRunData(workflowId) {
    const workflow = await this.#workflows.findOne(
      {
        id: workflowId,
      },
      {
        projection: {
          _id: 0,
          handlerId: 1,
          input: 1,
          failures: 1,
        },
      }
    );
    if (workflow) {
      return {
        handlerId: workflow.handlerId,
        input: workflow.input,
        failures: workflow.failures,
      };
    }
    throw new WorkflowNotFound(workflowId);
  }

  async #findStatusAndResult(workflowId) {
    const workflow = await this.#workflows.findOne(
      {
        id: workflowId,
      },
      {
        projection: {
          _id: 0,
          status: 1,
          result: 1,
        },
      }
    );
    if (!workflow) {
      throw new WorkflowNotFound(workflowId);
    }
    return {
      status: workflow.status,
      result: workflow.result,
    };
  }

  async #claim() {
    const now = new Date();
    const timeoutAt = new Date(now.getTime() + this.#timeoutInterval);
    const workflow = await this.#workflows.findOneAndUpdate(
      {
        status: { $in: ["idle", "running", "failed"] },
        timeoutAt: { $lt: now },
      },
      {
        $set: {
          status: "running",
          timeoutAt,
        },
      },
      {
        projection: {
          _id: 0,
          id: 1,
        },
      }
    );
    return workflow?.id;
  }

  async #setAsFinished(workflowId, result) {
    await this.#workflows.updateOne(
      {
        id: workflowId,
      },
      {
        $set: {
          status: "finished",
          result,
        },
      }
    );
  }

  async #updateStatus(workflowId, status, timeoutAt, failures) {
    await this.#workflows.updateOne(
      {
        id: workflowId,
      },
      {
        $set: {
          status,
          timeoutAt,
          failures,
        },
      }
    );
  }

  async #updateOutput(workflowId, stepId, output, timeoutAt) {
    await this.#workflows.updateOne(
      {
        id: workflowId,
      },
      {
        $set: {
          [`steps.${stepId}`]: output,
          timeoutAt,
        },
      }
    );
  }

  async #updateWakeUpAt(workflowId, napId, wakeUpAt, timeoutAt) {
    await this.#workflows.updateOne(
      {
        id: workflowId,
      },
      {
        $set: {
          [`naps.${napId}`]: wakeUpAt,
          timeoutAt,
        },
      }
    );
  }

  async #goSleep(pause) {
    return new Promise((resolve) => {
      setTimeout(() => {
        resolve();
      }, pause);
    });
  }
}
