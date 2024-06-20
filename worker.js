const throng = require('throng');
const Queue = require("bull");
const migration = require('./migrate_photos');
const migration_when_url_equals_photo_url = require('./migrate_photos_when_url_equals_photo_url');
const migration_when_only_url_is_not_null = require('./migrate_photos_when_only_url_is_not_null');
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
    settings: { 
      // If a worker doesn't report every 15 minutes, it is considered dead
      stalledInterval: 15*60*100,
      maxStalledCount: 100,
      // Sometimes it takes it more than 5 minutes - so we just skip that test
      skipStalledCheck: false,
    }
  });

  workQueue.process(maxJobsPerWorker, async (job) => {
    log.info(`Date ${job.data.date}: Running job ${job.id} for date ${job.data.date}`);
    job.progress(0);
    if (job.data.type == "urlEqualsPhotoUrl") {
      // This is for latest photos that were found with `url`==`photoUrl` fields that is a Cloudinary upload.
      // See: https://www.notion.so/mixtiles/Testing-at-the-End-of-the-Migration-86c492fdd3654a01baf59ba64f55ad5d?pvs=4#9e23d5d503e54250888ad0d27f75adc8
      return await migration_when_url_equals_photo_url.migratePhotosFromDate(job);
    } else if (job.data.type == "onlyUrlIsNotNull") {
      // This is for latest photos that were found with `url` field that is a Cloudinary upload.
      // See: https://www.notion.so/mixtiles/Testing-at-the-End-of-the-Migration-86c492fdd3654a01baf59ba64f55ad5d?pvs=4#a5dff727bbef47e5bec01490f9d5d0dc
      return await migration_when_only_url_is_not_null.migratePhotosFromDate(job);
    } else if (job.data.type == "regular") {
      return await migration.migratePhotosFromDate(job);
    } else {
      return { error: 'No migration type specified' };
    }
  });
}

// Initialize the clustered worker process
// See: https://devcenter.heroku.com/articles/node-concurrency for more info
throng({ workers: WEB_CONCURRENCY, start });
