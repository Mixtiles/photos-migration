const throng = require('throng');
const Queue = require("bull");
const backup_photos = require('./backup_photos');
const { redisOptions, WEB_CONCURRENCY } = require('./env_vars');
const { log } = require('./log');

// Spin up multiple processes to handle jobs to take advantage of more CPU cores
// See: https://devcenter.heroku.com/articles/node-concurrency for more info

// The maximum number of jobs each worker should process at once. This will need
// to be tuned for your application. If each job is mostly waiting on network 
// responses it can be much higher. If each job is CPU-intensive, it might need
// to be much lower.
const maxJobsPerWorker = 1;

function start() {

  // Connect to the named work queue
  const backupQueue = new Queue('backup', { 
    redis: redisOptions,
    settings: { 
      // If a worker doesn't report every 15 minutes, it is considered dead
alledInterval: 15*60*100,
      maxStalledCount: 100,
      // Sometimes it takes it more than 5 minutes - so we just skip that test
      skipStalledCheck: false,
    }
  });

  backupQueue.process(maxJobsPerWorker, async (job) => {
    log.info(`Backup photos: Running job ${job.id}`);
    job.progress(0);
    return await backup_photos.backupPhotos(job);
  });
}

// Initialize the clustered worker process
// See: https://devcenter.heroku.com/articles/node-concurrency for more info
throng({ workers: WEB_CONCURRENCY, start });
