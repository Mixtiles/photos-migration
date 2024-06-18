const express = require('express');
const Queue = require('bull');
const { redisOptions, PORT } = require('./env_vars');
const { log } = require('./log');

// Serve on PORT on Heroku and on localhost:5000 locally
// Connect to a local redis intance locally, and the Heroku-provided URL in production

const app = express();

// Create / Connect to a named work queue
const workQueue = new Queue('work', { 
  redis: redisOptions,
  settings: { 
    // If a worker doesn't report every 15 minutes, it is considered dead
    stalledInterval: 15*60*100,
    maxStalledCount: 100,
    // Sometimes it takes it more than 5 minutes - so we just skip that test
    skipStalledCheck: false,
  }
});

const deleteQueue = new Queue('delete', { 
  redis: redisOptions,
  settings: { 
    // If a worker doesn't report every 15 minutes, it is considered dead
    stalledInterval: 15*60*100,
    maxStalledCount: 100,
    // Sometimes it takes it more than 5 minutes - so we just skip that test
    skipStalledCheck: false,
  }
});


// Serve the two static assets
app.get('/', (req, res) => res.sendFile('index.html', { root: __dirname }));
app.get('/client.js', (req, res) => res.sendFile('client.js', { root: __dirname }));

// Kick off a new job by adding it to the work queue
app.post('/job/:date', async (req, res) => {
  // This would be where you could pass arguments to the job
  // Ex: workQueue.add({ url: 'https://www.heroku.com' })
  // Docs: https://github.com/OptimalBits/bull/blob/develop/REFERENCE.md#queueadd
  const date = req.params.date;
  if (!date.match(/^\d{4}-\d{2}-\d{2}$/)) {
    res.status(400).json("Invalid Date").end();
    return;
  }
  const job = await workQueue.add(
    {
      date: date,
    }, 
    {
      attempts: 1 // This tells Bull to attempt the job only once, with no retries after failure
    }
  );
  res.json({ id: job.id });
});

app.post('/job_delete/:num_jobs', async (req, res) => {
  // This would be where you could pass arguments to the job
  // Ex: workQueue.add({ url: 'https://www.heroku.com' })
  // Docs: https://github.com/OptimalBits/bull/blob/develop/REFERENCE.md#queueadd
  const num_jobs = req.params.num_jobs;
  const jobs = [];
  for (let i = 0; i < num_jobs; i++) {
    const job = await deleteQueue.add(
      {
        attempts: 1 // This tells Bull to attempt the job only once, with no retries after failure
      }
    );
    jobs.push(job);
  }
  res.json(jobs.map(job => job.id));
});
// Allows the client to query the state of a background job
app.get('/job/:id', async (req, res) => {
  const id = req.params.id;
  const job = await workQueue.getJob(id);

  if (job === null) {
    res.status(404).end();
  } else {
    const state = await job.getState();
    const progress = job._progress;
    const reason = job.failedReason;
    const data = job.data;
    res.json({ id, state, progress, reason, data });
  }
});

app.get('/job_delete/:id', async (req, res) => {
  const id = req.params.id;
  const job = await deleteQueue.getJob(id);

  if (job === null) {
    res.status(404).end();
  } else {
    const state = await job.getState();
    const progress = job._progress;
    const reason = job.failedReason;
    const data = job.data;
    res.json({ id, state, progress, reason, data });
  }
});

// You can listen to global events to get notified when jobs are processed
workQueue.on('global:completed', (jobId, result) => {
  log.info(`Job ${jobId} completed with result ${result}`);
});

app.listen(PORT, () => log.info("Server started!"));
