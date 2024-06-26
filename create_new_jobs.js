const { createClient } = require('redis');
const {
    REDIS_URL,
    redisOptions,
    MAX_ACTIVE_JOBS
} = require('./env_vars');
const { log } = require('./log');
const Queue = require('bull');

const redisClient = createClient({ url: REDIS_URL})

function getPreviousDays(dateString, numDays) {
    const result = [];
    const date = new Date(dateString);
  
    for (let i = 1; i <= numDays; i++) {
      const previousDate = new Date(date.getTime());
      previousDate.setDate(date.getDate() - i);
      result.push(previousDate.toISOString().split('T')[0]);
    }
  
    return result;
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

async function getLastDate() {
    let lastDate = new Date();
    const keys = await redisClient.keys("bull:work:*")
    for (const key of keys) {
        if (key.match(/bull:work:(\d)+$/)) {
            log.info(`Create New Jobs - Checking key ${key}`)
            const dateStr = JSON.parse(await redisClient.hGet(key, "data"))["date"]
            const date = new Date(dateStr);
            if (date < lastDate) {
                lastDate = date;
            }
        }
    }
    return lastDate.toISOString().split('T')[0];
}
  
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

    lastDate = await getLastDate();
    log.info(`Create New Jobs - Last date: ${lastDate}`);
    const previousDays = getPreviousDays(lastDate, MAX_ACTIVE_JOBS - numActiveJobs);
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
