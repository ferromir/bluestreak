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
  #steps;
  #naps;
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
    this.#steps = null;
    this.#naps = null;
    this.#timeoutInterval = params.timeoutInterval || 10_000;
    this.#pollInterval = params.pollInterval || 5_000;
    this.#waitRetryInterval = params.waitRetryInterval || 1_000;
    this.#errorCallback = params.errorCallback;
    this.#maxFailures = params.maxFailures;
    this.#shouldStop = params.shouldStop;
    this.#handlers = new Map();
  }

  getParams() {
    return {
      dbUrl: this.#dbUrl,
      dbName: this.#dbName,
      timeoutInterval: this.#timeoutInterval,
      pollInterval: this.#pollInterval,
      waitRetryInterval: this.#waitRetryInterval,
      maxFailures: this.#maxFailures,
    };
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
   * Creates three collections:
   * - workflows: Stores workflow state (status, timeoutAt, failures, input, result)
   * - steps: Stores individual step outputs separately to avoid document size limits
   * - naps: Stores sleep/nap state separately to avoid document size limits
   *
   * @returns {Promise<void>}
   */
  async init() {
    this.#client = new MongoClient(this.#dbUrl);
    const db = this.#client.db(this.#dbName);
    this.#workflows = db.collection("workflows");
    await this.#workflows.createIndex({ workflowId: 1 }, { unique: true });
    await this.#workflows.createIndex({ status: 1, timeoutAt: 1 });
    this.#steps = db.collection("steps");
    await this.#steps.createIndex(
      { workflowId: 1, stepId: 1 },
      { unique: true }
    );
    this.#naps = db.collection("naps");
    await this.#naps.createIndex({ workflowId: 1, napId: 1 }, { unique: true });
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
   * @returns {Promise<boolean>} Returns true if workflow was created, false if it already exists
   */
  async start(workflowId, handlerId, input) {
    try {
      await this.#insert(workflowId, handlerId, input);
      return true;
    } catch (err) {
      if (err.name === "MongoServerError" && err.code === 11000) {
        return false;
      }
      throw err;
    }
  }

  /**
   * Finds a workflow by its ID.
   *
   * @param {string} workflowId - The ID of the workflow to find
   * @returns {Promise<Object|null>} The workflow document or null if not found
   */
  async findWorkflow(workflowId) {
    return await this.#workflows.findOne({ workflowId });
  }

  /**
   * Finds a step by workflow ID and step ID.
   *
   * @param {string} workflowId - The ID of the workflow
   * @param {string} stepId - The ID of the step
   * @returns {Promise<Object|null>} The step document or null if not found
   */
  async findStep(workflowId, stepId) {
    return await this.#steps.findOne({ workflowId, stepId });
  }

  /**
   * Finds a nap (sleep) by workflow ID and nap ID.
   *
   * @param {string} workflowId - The ID of the workflow
   * @param {string} napId - The ID of the nap
   * @returns {Promise<Object|null>} The nap document or null if not found
   */
  async findNap(workflowId, napId) {
    return await this.#naps.findOne({ workflowId, napId });
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

  /**
   * Executes a workflow handler for the given workflow ID.
   *
   * Retrieves the workflow's run data, finds the registered handler, and invokes it.
   * If the handler throws an error, the workflow is marked as failed or aborted
   * (depending on maxFailures setting) and will be retried after waitRetryInterval.
   *
   * @param {string} workflowId - The ID of the workflow to run
   * @returns {Promise<void>}
   * @throws {WorkflowNotFound} If the workflow doesn't exist
   * @throws {HandlerNotFound} If the handler is not registered
   */
  async #run(workflowId) {
    const runData = await this.#findRunData(workflowId);
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

  /**
   * Creates a step function bound to a specific workflow.
   *
   * Steps are idempotent: if a step has already been executed, its cached output
   * is returned. Otherwise, the function is executed and its output is persisted
   * in the steps collection before returning.
   *
   * Note: Step persistence and timeout updates are not atomic, but this is acceptable
   * as the worst case is early workflow retry on crash.
   *
   * @param {string} workflowId - The ID of the workflow
   * @returns {Function} A step function that takes (stepId, fn) and returns the step output
   */
  #step(workflowId) {
    return async function (stepId, fn) {
      let output = await this.#findOutput(workflowId, stepId);
      if (output !== undefined) {
        return output;
      }
      output = await fn();
      const now = new Date();
      const timeoutAt = new Date(now.getTime() + this.#timeoutInterval);
      await this.#insertStep(workflowId, stepId, output);
      await this.#updateTimeoutAt(workflowId, timeoutAt);
      return output;
    };
  }

  /**
   * Creates a sleep function bound to a specific workflow.
   *
   * Sleeps are idempotent and durable: if a nap has already been started, it calculates
   * the remaining sleep time. The wakeUpAt time is persisted in the naps collection to
   * survive worker restarts.
   *
   * Note: Nap persistence and timeout updates are not atomic, but this is acceptable
   * as the worst case is early workflow retry on crash.
   *
   * @param {string} workflowId - The ID of the workflow
   * @returns {Function} A sleep function that takes (napId, ms) and sleeps for the duration
   */
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
      await this.#insertNap(workflowId, napId, wakeUpAt);
      await this.#updateTimeoutAt(workflowId, timeoutAt);
      await this.#goSleep(ms);
    };
  }

  /**
   * Inserts a new workflow into the workflows collection.
   *
   * @param {string} workflowId - The unique workflow ID
   * @param {string} handlerId - The handler ID to execute
   * @param {any} input - The input data for the workflow
   * @returns {Promise<void>}
   * @throws {MongoServerError} If a workflow with the same ID already exists (E11000)
   */
  async #insert(workflowId, handlerId, input) {
    const now = new Date();
    await this.#workflows.insertOne({
      workflowId,
      handlerId,
      input,
      failures: 0,
      status: "idle",
      timeoutAt: now,
    });
  }

  /**
   * Finds the output of a previously executed step.
   *
   * @param {string} workflowId - The workflow ID
   * @param {string} stepId - The step ID
   * @returns {Promise<any>} The step output, or undefined if not found
   */
  async #findOutput(workflowId, stepId) {
    const step = await this.#steps.findOne({
      workflowId,
      stepId,
    });
    return step ? step.output : undefined;
  }

  /**
   * Finds the wakeUpAt time for a previously started nap.
   *
   * @param {string} workflowId - The workflow ID
   * @param {string} napId - The nap ID
   * @returns {Promise<Date|undefined>} The wakeUpAt Date, or undefined if not found
   */
  async #findWakeUpAt(workflowId, napId) {
    const nap = await this.#naps.findOne({
      workflowId,
      napId,
    });
    return nap ? nap.wakeUpAt : undefined;
  }

  /**
   * Finds the data needed to run a workflow (handlerId, input, failures).
   *
   * @param {string} workflowId - The workflow ID
   * @returns {Promise<Object>} Object with handlerId, input, and failures
   * @throws {WorkflowNotFound} If the workflow doesn't exist
   */
  async #findRunData(workflowId) {
    const workflow = await this.#workflows.findOne(
      {
        workflowId,
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

  /**
   * Finds the status and result of a workflow.
   *
   * @param {string} workflowId - The workflow ID
   * @returns {Promise<Object>} Object with status and result
   * @throws {WorkflowNotFound} If the workflow doesn't exist
   */
  async #findStatusAndResult(workflowId) {
    const workflow = await this.#workflows.findOne(
      {
        workflowId,
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

  /**
   * Atomically claims a workflow that is ready to run.
   *
   * Looks for workflows with status "idle", "running", or "failed" that have
   * timed out (timeoutAt < now), updates their status to "running" and sets
   * a new timeout.
   *
   * @returns {Promise<string|undefined>} The workflow ID if claimed, undefined otherwise
   */
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
          workflowId: 1,
        },
      }
    );
    return workflow?.workflowId;
  }

  /**
   * Marks a workflow as finished and stores its result.
   *
   * @param {string} workflowId - The workflow ID
   * @param {any} result - The workflow result
   * @returns {Promise<void>}
   */
  async #setAsFinished(workflowId, result) {
    await this.#workflows.updateOne(
      {
        workflowId,
      },
      {
        $set: {
          status: "finished",
          result,
        },
      }
    );
  }

  /**
   * Updates the status, timeoutAt, and failure count of a workflow.
   *
   * @param {string} workflowId - The workflow ID
   * @param {string} status - The new status ("failed" or "aborted")
   * @param {Date} timeoutAt - The new timeout timestamp
   * @param {number} failures - The updated failure count
   * @returns {Promise<void>}
   */
  async #updateStatus(workflowId, status, timeoutAt, failures) {
    await this.#workflows.updateOne(
      {
        workflowId,
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

  /**
   * Updates only the timeoutAt field of a workflow.
   *
   * @param {string} workflowId - The workflow ID
   * @param {Date} timeoutAt - The new timeout timestamp
   * @returns {Promise<void>}
   */
  async #updateTimeoutAt(workflowId, timeoutAt) {
    await this.#workflows.updateOne(
      {
        workflowId,
      },
      {
        $set: {
          timeoutAt,
        },
      }
    );
  }

  /**
   * Inserts a step output into the steps collection using upsert.
   *
   * Uses $setOnInsert to make the operation idempotent - if the step already
   * exists (from a previous attempt before crash), it won't be modified.
   *
   * @param {string} workflowId - The workflow ID
   * @param {string} stepId - The step ID
   * @param {any} output - The step output to store
   * @returns {Promise<void>}
   */
  async #insertStep(workflowId, stepId, output) {
    await this.#steps.updateOne(
      {
        workflowId,
        stepId,
      },
      {
        $setOnInsert: {
          workflowId,
          stepId,
          output,
        },
      },
      {
        upsert: true,
      }
    );
  }

  /**
   * Inserts a nap (sleep) into the naps collection using upsert.
   *
   * Uses $setOnInsert to make the operation idempotent - if the nap already
   * exists (from a previous attempt before crash), it won't be modified.
   *
   * @param {string} workflowId - The workflow ID
   * @param {string} napId - The nap ID
   * @param {Date} wakeUpAt - The time to wake up
   * @returns {Promise<void>}
   */
  async #insertNap(workflowId, napId, wakeUpAt) {
    await this.#naps.updateOne(
      {
        workflowId,
        napId,
      },
      {
        $setOnInsert: {
          workflowId,
          napId,
          wakeUpAt,
        },
      },
      {
        upsert: true,
      }
    );
  }

  /**
   * Sleeps for a specified duration using setTimeout.
   *
   * @param {number} pauseInterval - The duration to sleep in milliseconds
   * @returns {Promise<void>}
   */
  async #goSleep(pauseInterval) {
    return new Promise((resolve) => {
      setTimeout(() => {
        resolve();
      }, pauseInterval);
    });
  }
}
