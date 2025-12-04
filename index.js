import { MongoClient } from "mongodb";

export class WorkflowNotFound extends Error {
  constructor(workflowId) {
    super(`workflow not found: ${workflowId}`);
    this.name = 'WorkflowNotFound';
    this.workflowId = workflowId;
  }
}

export class HandlerNotFound extends Error {
  constructor(handlerId) {
    super(`handler not found: ${handlerId}`);
    this.name = 'HandlerNotFound';
    this.handlerId = handlerId;
  }
}

export class WaitTimeout extends Error {
  constructor(workflowId) {
    super(`wait timeout: ${workflowId}`);
    this.name = 'WaitTimeout';
    this.workflowId = workflowId;
  }
}

export class Bluestreak {
  #dbUrl
  #dbName
  #client
  #workflows
  #shouldStop
  #timeoutInterval
  #pollInterval
  #waitRetryInterval
  #errorHandler
  #maxFailures
  #handlers

  constructor(params) {
    this.#dbUrl = params.dbUrl || 'mongodb://localhost:27017';
    this.#dbName = params.dbName || 'bluestreak';
    this.#client = null;
    this.#workflows = null;
    this.#shouldStop = params.shouldStop;
    this.#timeoutInterval = params.timeoutInterval || 10_000;
    this.#pollInterval = params.pollInterval || 5_000;
    this.#waitRetryInterval = params.waitRetryInterval || 1_000;
    this.#errorHandler = params.errorHandler;
    this.#maxFailures = params.maxFailures;
    this.#handlers = new Map();
  }

  async init() {
    this.#client = new MongoClient(this.#dbUrl);
    const db = this.#client.db(this.#dbName);
    this.#workflows = db.collection('workflows');
    await this.#workflows.createIndex({ id: 1 }, { unique: true });
    await this.#workflows.createIndex({ status: 1, timeoutAt: 1 });
  }

  async close() {
    await this.#client.close();
  }

  registerHandler(handlerId, handler) {
    this.#handlers.set(handlerId, handler);
  }

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

  async wait(workflowId, retries, pauseInterval) {
    for (let i = 0; i < retries; i++) {
      const data = await this.#findStatusAndResult(workflowId);
      if (data.status === 'finished') {
        return data.result;
      }
      await this.#goSleep(pauseInterval);
    }
    throw new WaitTimeout(workflowId);
  }

  async poll() {
    while(!this.#shouldStop()) {
      const workflowId = await this.#claim();
      if (workflowId) {
        this.#run(workflowId);
      } else {
        await this.#goSleep(this.#pollInterval);
      }
    }
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
      sleep: this.#sleep(workflowId).bind(this)
    };
    try {
      const result = await handler(ctx, runData.input);
      await this.#setAsFinished(workflowId, result);
    } catch (err) {
      const failures = runData.failures + 1;
      // if maxFailures is undefined, it will never be aborted.
      const status = failures > this.#maxFailures ? "aborted" : "failed";
      const now = new Date();
      const timeoutAt = new Date(now.getTime() + this.#waitRetryInterval);
      await this.#updateStatus(workflowId, status, timeoutAt, failures);
      if (this.#errorHandler) {
        this.#errorHandler(workflowId, err);
      }
    }
  }

  #step(workflowId) {
    return async function (stepId, fn) {
      let output = await this.#findOutput(workflowId, stepId);
      if (!(output === undefined)) {
        return output;
      }
      output = await fn();
      const now = new Date();
      const timeoutAt = new Date(now.getTime() + this.#timeoutInterval);
      await this.#updateOutput(workflowId, stepId, output, timeoutAt);
      return output;      
    }
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
    }
  }

  async #insert(workflowId, handlerId, input) {
    await this.#workflows.insertOne({
      id: workflowId,
      handlerId: handlerId,
      input,
      status: "idle",
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
      },
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
      },
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
      },
    );
    if (workflow) {
      return {
        handlerId: workflow.handlerId,
        input: workflow.input,
        failures: workflow.failures,
      };
    }
    return undefined;
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
          result: 1
        },
      },
    );
    if (!workflow) {
      throw new WorkflowNotFound(workflowId);
    }
    return {
      status: workflow.status,
      result: workflow.result
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
      },
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
          result
        },
      },
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
          failures
        },
      },
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
      },
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
      },
    );
  }

  async #goSleep(pause) {
    return new Promise((resolve) => {
      setTimeout(() => { resolve()}, pause);
    });
  }
}