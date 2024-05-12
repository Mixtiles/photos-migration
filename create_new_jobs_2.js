const { createClient } = require('redis');
const {
    REDIS_URL,
    redisOptions,
    MAX_ACTIVE_JOBS
} = require('./env_vars');
const { log } = require('./log');
const Queue = require('bull');

LIST_NAME = "datesToMigrates"

const redisClient = createClient({ url: REDIS_URL})

async function getNextDays(numDays) {
    const result = [];
  
    for (let i = 1; i <= numDays; i++) {
        const nextDay = await redisClient.lPop(LIST_NAME)
        result.push(nextDay);
    }
  
    return result.filter(date => date !== null)
}

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

async function runNewJobs() {
    log.info(`Create New Jobs - Starting...`);

    await redisClient.connect();
    numActiveJobs = await redisClient.lLen(`bull:work:active`);

    log.info(`Create New Jobs - Number of active jobs: ${numActiveJobs}`);
    if (numActiveJobs >= MAX_ACTIVE_JOBS) {
        log.info(`Create New Jobs - There are enough active jobs (${numActiveJobs}) - more than or exactly the maximum (${MAX_ACTIVE_JOBS}). Skipping...`)
        await redisClient.disconnect();
        process.exit(0);
        return;
    } else {
        log.info(`Create New Jobs - Going to create ${MAX_ACTIVE_JOBS - numActiveJobs} new jobs...`)
    }

    const previousDays = await getNextDays(MAX_ACTIVE_JOBS - numActiveJobs);

    if (previousDays.length === 0) {
        log.info(`Create New Jobs - No more dates to process. Exiting...`)
        await redisClient.disconnect();
        process.exit(0);
        return;
    }

    for (const date of previousDays) {
        log.info(`Create New Jobs - Running job for ${date}`);
        const job = await workQueue.add(
            {
              date: date,
            }, 
            {
              attempts: 1 // This tells Bull to attempt the job only once, with no retries after failure
            }
        );
        log.info(`Create New Jobs - Job ${job.id} created for date ${date}`);
    }
    log.info(`Create New Jobs - Finished!`);
    await redisClient.disconnect();
    process.exit(0);
  }

runNewJobs()
