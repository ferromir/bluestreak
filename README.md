# Bluestreak

[![CI](https://github.com/ferromir/bluestreak/actions/workflows/ci.yml/badge.svg)](https://github.com/ferromir/bluestreak/actions/workflows/ci.yml)
[![Coverage](.github/badges/coverage.svg)](https://github.com/ferromir/bluestreak/actions/workflows/ci.yml)
[![Branches](.github/badges/branches.svg)](https://github.com/ferromir/bluestreak/actions/workflows/ci.yml)
[![npm version](https://badge.fury.io/js/bluestreak.svg)](https://www.npmjs.com/package/bluestreak)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Lightweight durable execution powered by MongoDB

## What is Bluestreak?

Bluestreak is a lightweight durable execution library that enables you to build reliable, long-running workflows using just MongoDB. It provides the core capabilities of durable execution frameworks like Temporal or AWS Step Functions, but with dramatically simpler deployment and operational requirements.

Bluestreak uses a three-collection MongoDB architecture (workflows, steps, naps) that allows workflows to scale without hitting document size limits while maintaining simplicity and performance.

**Durable execution** means your workflows can:

- Survive crashes and restarts
- Automatically retry failed operations
- Execute idempotent steps that safely replay
- Sleep for arbitrary durations without tying up resources
- Maintain execution state across process boundaries

## Why Choose Bluestreak?

### No Additional Services or Infrastructure

Unlike enterprise durable execution platforms, Bluestreak requires **only MongoDB** - a database many applications already use. No need to:

- ❌ Deploy additional services (Temporal Server, AWS Step Functions, etc.)
- ❌ Manage complex distributed systems
- ❌ Set up separate infrastructure for workflow execution
- ❌ Learn proprietary domain-specific languages or configuration formats
- ❌ Pay for additional cloud services

### Lightweight and Simple

- **Just JavaScript** - Write workflows in plain JavaScript/TypeScript with async/await
- **Single Dependency** - Only requires the MongoDB driver
- **Small Footprint** - Minimal memory and CPU overhead
- **Easy Deployment** - Works anywhere Node.js and MongoDB run
- **Simple Mental Model** - Workflows, handlers, steps, and sleeps - that's it

### Production-Ready Features

Despite its simplicity, Bluestreak includes essential production features:

- ✅ Automatic retries with configurable failure limits
- ✅ Idempotent step execution with result caching
- ✅ Durable sleeps that survive process restarts
- ✅ Fire-and-forget workflow execution
- ✅ Workflow status tracking
- ✅ Error callbacks for monitoring and alerting
- ✅ Timeout handling for long-running operations

## Installation

```bash
npm install @ferromir/bluestreak
```

## Quick Start

```javascript
import { Bluestreak } from "bluestreak";

// Create a Bluestreak instance
const bluestreak = new Bluestreak({
  dbUrl: "mongodb://localhost:27017",
  dbName: "myapp",
  maxFailures: 3,
});

// Register a workflow handler
bluestreak.registerHandler("send-welcome-email", async (ctx, input) => {
  // Step 1: Create user account (idempotent)
  const userId = await ctx.step("create-user", async () => {
    return await createUserInDatabase(input.email, input.name);
  });

  // Step 2: Send welcome email (idempotent)
  await ctx.step("send-email", async () => {
    await sendEmail(input.email, "Welcome!", "Thanks for signing up!");
  });

  // Step 3: Wait 24 hours before sending follow-up
  await ctx.sleep("wait-day", 24 * 60 * 60 * 1000);

  // Step 4: Send follow-up email
  await ctx.step("send-followup", async () => {
    await sendEmail(input.email, "How's it going?", "Need any help?");
  });

  return { userId, status: "completed" };
});

// Initialize the connection
await bluestreak.init();

// Start the workflow execution loop
const shouldStop = () => false; // Run until manually stopped
bluestreak.shouldStop = shouldStop;
await bluestreak.poll();
```

To start a workflow:

```javascript
// Start a workflow
await bluestreak.start("user-123-onboarding", "send-welcome-email", {
  email: "user@example.com",
  name: "Alice",
});

// Wait for completion (optional)
const result = await bluestreak.wait("user-123-onboarding", 100, 1000);
console.log(result); // { userId: "...", status: "completed" }
```

## Core Concepts

### Workflows

A workflow is a long-running process that coordinates multiple operations. Each workflow:

- Has a unique ID (you provide this)
- Executes a handler function
- Maintains persistent state in MongoDB
- Can be retried on failure
- Tracks execution progress through steps and sleeps

### Handlers

Handlers are the functions that implement your workflow logic. Register handlers with `registerHandler()`:

```javascript
bluestreak.registerHandler("order-fulfillment", async (ctx, input) => {
  // Your workflow logic here
  const { orderId, items } = input;

  // Reserve inventory
  const reservation = await ctx.step("reserve-inventory", async () => {
    return await inventoryService.reserve(items);
  });

  // Charge customer
  await ctx.step("charge-customer", async () => {
    await paymentService.charge(input.customerId, input.total);
  });

  // Ship items
  await ctx.step("ship-items", async () => {
    await shippingService.createShipment(orderId, items);
  });

  return { orderId, status: "fulfilled" };
});
```

### Steps - Idempotent Operations

Steps are the building blocks of workflows. Each step:

- Must have a unique ID within the workflow
- Executes exactly once (even if the workflow retries)
- Caches its result in a dedicated `steps` collection in MongoDB
- Returns the cached result on subsequent executions
- Stored separately from the workflow document to avoid size limits

```javascript
// If this workflow crashes after step 1 completes,
// step 1 will NOT re-execute - it returns the cached result
const userId = await ctx.step("create-user", async () => {
  return await database.createUser(email); // Only runs once
});

const subscription = await ctx.step("create-subscription", async () => {
  // This step only runs once too
  return await stripe.createSubscription(userId);
});
```

**Why steps matter:** Without idempotent steps, a workflow that crashes and retries might create duplicate users, charge credit cards twice, or send multiple emails. Steps prevent this by caching results.

**Storage:** Step outputs are stored in a separate `steps` collection with a compound index on `(workflowId, stepId)`, allowing workflows to have unlimited steps without hitting MongoDB document size limits.

### Sleep - Durable Delays

Sleep lets workflows pause for arbitrary durations without consuming resources:

```javascript
// Wait 7 days before sending reminder
await ctx.sleep("wait-week", 7 * 24 * 60 * 60 * 1000);

// The workflow is persisted in MongoDB during the sleep
// Your process can restart, and the workflow resumes at the right time
```

Sleeps are durable - if your process crashes during a sleep, the workflow resumes correctly when the poll loop restarts.

**Storage:** Sleep state (wakeUpAt times) is stored in a separate `naps` collection with a compound index on `(workflowId, napId)`, allowing workflows to have unlimited sleeps without hitting MongoDB document size limits.

### Polling

The poll loop is what executes workflows. It continuously:

1. Claims idle workflows from MongoDB
2. Executes their handlers in a fire-and-forget pattern
3. Retries failed workflows according to `maxFailures`
4. Updates workflow state after each step

```javascript
let running = true;

bluestreak
  .poll({
    shouldStop: () => !running,
  })
  .catch((err) => {
    console.error("Poll loop error:", err);
  });

// Gracefully stop polling
process.on("SIGTERM", () => {
  running = false;
});
```

## Configuration

```javascript
const bluestreak = new Bluestreak({
  // MongoDB connection
  dbUrl: "mongodb://localhost:27017",
  dbName: "bluestreak",

  // Timeout before a claimed workflow is released (ms)
  timeoutInterval: 10000,

  // Time to wait between poll attempts when queue is empty (ms)
  pollInterval: 5000,

  // Time to wait before retrying a failed workflow (ms)
  waitRetryInterval: 1000,

  // Maximum failures before aborting a workflow
  maxFailures: 5,

  // Error callback for monitoring
  errorCallback: (workflowId, error) => {
    console.error(`Workflow ${workflowId} failed:`, error);
    // Send to monitoring service, log aggregator, etc.
  },

  // Callback to stop polling
  shouldStop: () => process.env.SHUTDOWN === "true",
});
```

## API Reference

### `Bluestreak` Class

#### `constructor(params)`

Creates a new Bluestreak instance.

**Parameters:**

- `dbUrl` (string, optional): MongoDB connection URL. Default: `"mongodb://localhost:27017"`
- `dbName` (string, optional): MongoDB database name. Default: `"bluestreak"`
- `timeoutInterval` (number, optional): Timeout for workflow execution in ms. Default: `10000`
- `pollInterval` (number, optional): Interval between polls when queue is empty in ms. Default: `5000`
- `waitRetryInterval` (number, optional): Interval before retrying failed workflows in ms. Default: `1000`
- `errorCallback` (function, optional): Callback invoked when workflows fail. Signature: `(workflowId: string, error: Error) => void`
- `maxFailures` (number, optional): Maximum failures before aborting. Default: unlimited
- `shouldStop` (function, optional): Callback to determine when to stop polling. Signature: `() => boolean`

#### `registerHandler(handlerId, handler)`

Registers a workflow handler.

**Parameters:**

- `handlerId` (string): Unique identifier for the handler
- `handler` (function): Handler function with signature `(ctx, input) => Promise<any>`

#### `async init()`

Initializes the MongoDB connection and creates required indexes. Must be called before polling or starting workflows.

**Collections created:**
- `workflows` - Stores workflow metadata (status, timeoutAt, failures, input, result)
- `steps` - Stores step outputs separately (indexed by workflowId + stepId)
- `naps` - Stores sleep state separately (indexed by workflowId + napId)

This three-collection architecture prevents workflows from hitting MongoDB's 16MB document size limit.

#### `async close()`

Closes the MongoDB connection.

#### `async start(workflowId, handlerId, input)`

Starts a new workflow execution.

**Parameters:**

- `workflowId` (string): Unique identifier for this workflow instance
- `handlerId` (string): ID of the registered handler to execute
- `input` (any): Input data passed to the handler

**Returns:** `true` if workflow was created, `false` if it already exists

#### `async wait(workflowId, retries, pauseInterval)`

Waits for a workflow to complete by polling its status.

**Parameters:**

- `workflowId` (string): ID of the workflow to wait for
- `retries` (number): Number of times to check status
- `pauseInterval` (number): Milliseconds to wait between checks

**Returns:** The result returned by the workflow handler

**Throws:**

- `WaitTimeout`: If workflow doesn't complete within retry limit
- `WorkflowNotFound`: If workflow doesn't exist

#### `async poll()`

Starts the workflow execution loop. Continues until `shouldStop()` returns true.

**Throws:**

- `HandlerNotFound`: If a workflow references an unregistered handler
- `WorkflowNotFound`: If a claimed workflow is not found

### Workflow Context

The context object (`ctx`) passed to workflow handlers provides:

#### `ctx.step(stepId, fn)`

Executes an idempotent step.

**Parameters:**

- `stepId` (string): Unique identifier for this step within the workflow
- `fn` (function): Async function to execute. Signature: `() => Promise<any>`

**Returns:** The result of `fn()`, or the cached result if already executed

#### `ctx.sleep(napId, ms)`

Sleeps for a duration.

**Parameters:**

- `napId` (string): Unique identifier for this sleep within the workflow
- `ms` (number): Milliseconds to sleep

### Error Classes

#### `WorkflowNotFound`

Thrown when a workflow ID is not found in the database.

#### `HandlerNotFound`

Thrown when a handler ID is not registered.

#### `WaitTimeout`

Thrown when `wait()` exceeds its retry limit.

## Examples

### Email Campaign with Delays

```javascript
bluestreak.registerHandler("email-campaign", async (ctx, input) => {
  const { userId, campaignId } = input;

  // Send welcome email
  await ctx.step("email-1", async () => {
    await sendEmail(userId, "Welcome to our product!");
  });

  // Wait 3 days
  await ctx.sleep("pause-1", 3 * 24 * 60 * 60 * 1000);

  // Send feature highlight
  await ctx.step("email-2", async () => {
    await sendEmail(userId, "Check out these features!");
  });

  // Wait 7 days
  await ctx.sleep("pause-2", 7 * 24 * 60 * 60 * 1000);

  // Send upgrade offer
  await ctx.step("email-3", async () => {
    await sendEmail(userId, "Upgrade to premium!");
  });

  return { campaignId, emailsSent: 3 };
});
```

### Payment Processing with Retries

```javascript
bluestreak.registerHandler("process-payment", async (ctx, input) => {
  const { orderId, amount, customerId } = input;

  // Validate order
  const order = await ctx.step("validate-order", async () => {
    return await orderService.get(orderId);
  });

  // Charge payment method
  const charge = await ctx.step("charge-payment", async () => {
    // This will automatically retry if it fails (up to maxFailures)
    return await paymentGateway.charge(customerId, amount);
  });

  // Update order status
  await ctx.step("update-order", async () => {
    await orderService.updateStatus(orderId, "paid");
  });

  // Send confirmation email
  await ctx.step("send-confirmation", async () => {
    await emailService.send(order.email, "Payment received!");
  });

  return { orderId, chargeId: charge.id, status: "completed" };
});

// Configure with automatic retries
const bluestreak = new Bluestreak({
  maxFailures: 5,
  waitRetryInterval: 5000, // Retry after 5 seconds
  errorCallback: (workflowId, error) => {
    logger.error("Payment workflow failed", { workflowId, error });
  },
});
```

### Data Pipeline with Dependencies

```javascript
bluestreak.registerHandler("daily-analytics", async (ctx, input) => {
  const { date } = input;

  // Extract data from sources
  const rawData = await ctx.step("extract-data", async () => {
    return await dataWarehouse.extract(date);
  });

  // Transform data
  const transformedData = await ctx.step("transform-data", async () => {
    return await analyticsEngine.transform(rawData);
  });

  // Load into analytics database
  await ctx.step("load-data", async () => {
    await analyticsDB.bulkInsert(transformedData);
  });

  // Generate reports
  const reports = await ctx.step("generate-reports", async () => {
    return await reportGenerator.create(date);
  });

  // Notify stakeholders
  await ctx.step("notify-stakeholders", async () => {
    await emailService.sendReports(reports);
  });

  return { date, recordsProcessed: transformedData.length };
});
```

## Error Handling

Bluestreak provides automatic retry logic for transient failures:

```javascript
const bluestreak = new Bluestreak({
  maxFailures: 3, // Retry up to 3 times
  waitRetryInterval: 2000, // Wait 2 seconds between retries
  errorCallback: (workflowId, error) => {
    // Log errors for monitoring
    monitoring.recordError(workflowId, error);

    // Send alerts for critical workflows
    if (workflowId.startsWith("critical-")) {
      alerting.sendPage(error);
    }
  },
});
```

**Workflow States:**

- `idle` - Waiting to be claimed
- `running` - Currently executing
- `failed` - Failed but will retry
- `aborted` - Failed too many times (exceeds maxFailures)
- `finished` - Completed successfully

## Contributing

See [CONTRIBUTING.md](.github/CONTRIBUTING.md) for development setup, testing requirements, and contribution guidelines.

## License

MIT
