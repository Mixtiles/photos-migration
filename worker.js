const throng = require('throng');
const Queue = require("bull");
const migration = require('./migrate_photos');
const { redisOptions, WEB_CONCURRENCY } = require('./env_vars');
const { log } = require('./log');
const { getFilestackHandleIdToPath } = require('./filestack');

// Spin up multiple processes to handle jobs to take advantage of more CPU cores
// See: https://devcenter.heroku.com/articles/node-concurrency for more info

// The maximum number of jobs each worker should process at once. This will need
// to be tuned for your application. If each job is mostly waiting on network 
// responses it can be much higher. If each job is CPU-intensive, it might need
// to be much lower.
const maxJobsPerWorker = 50;

getFilestackHandleIdToPath();

function start() {

  // Connect to the named work queue
  const workQueue = new Queue('work', { 
    redis: redisOptions,
    // If a worker doesn't report every 5 minutes, it is considered dead
    settings: { stalledInterval: 5*60*100 }
  });

  workQueue.process(maxJobsPerWorker, async (job) => {
    log.info(`Date ${job.data.date}: Running job ${job.id} for date ${job.data.date}`);
    job.progress(0);
    return await migration.migratePhotosFromDate(job);
  });
}

// Initialize the clustered worker process
// See: https://devcenter.heroku.com/articles/node-concurrency for more info
throng({ workers: WEB_CONCURRENCY, start });
