const express = require('express');
const Queue = require('bull');
const { redisOptions, PORT } = require('./env_vars');

// Serve on PORT on Heroku and on localhost:5000 locally
// Connect to a local redis intance locally, and the Heroku-provided URL in production

const app = express();

// Create / Connect to a named work queue
const workQueue = new Queue('work', redisOptions);

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

// You can listen to global events to get notified when jobs are processed
workQueue.on('global:completed', (jobId, result) => {
  console.log(`Job completed with result ${result}`);
});

app.listen(PORT, () => console.log("Server started!"));
