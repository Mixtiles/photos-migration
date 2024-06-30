const { createClient } = require("redis");
const { log } = require("./log");
const cloudinary = require("cloudinary");
const { uploadStreamToS3 } = require('./common');
const axios = require('axios');

const {
  REDIS_URL,
  BACKUP_BATCH_SIZE,
  BACKUP_NUM_PHOTOS,
  BACKUP_BUCKET,
} = require("./env_vars");

const BACKEDUP_PHOTOS_SET = "backedupPhotos";
const BACKEDUP_PHOTOS_ERRORS_SET = "backedupPhotosErrors";
const PHOTOS_TO_BACKUP_SET = "photosToBackup";

async function backupToS3(publicId, bucketName) {
  const downloadUrl = `https://res.cloudinary.com/mixtiles/image/upload/${publicId}`
  const response = await axios.get(downloadUrl, { responseType: 'stream' })
  const format = response.headers['content-type'].split('/')[1]

  return (
      await uploadStreamToS3({
          stream: response.data,
          filename: `${publicId}.${format}`,
          bucketName: bucketName,
      })
  ).url
}

async function backupPhoto(photo, job) {
  try {
    const url = await backupToS3(photo, BACKUP_BUCKET);
    log.info(
      `Job ${job.id}: Photo backed up successfully - ${photo}: ${url}`,
    );
    return "ok";
  } catch (error) {
    if (error.message ===  "Request failed with status code 404") {
      log.error(
        `Job ${job.id}: Error backing up photo - not found - ${photo}: ${JSON.stringify(error)}`,
      );
      return "not found";
    } else {
      log.error(
        `Job ${job.id}: Error backing up photo - exception - ${photo}: ${JSON.stringify(error)}`,
      );
      return "exception";
    }
  }
}

async function updateResult(photo, result, redisClient) {
  if (result == "ok" || result == "not found") {
    await redisClient.sAdd(BACKEDUP_PHOTOS_SET, photo);
  } else if (result == "unexpected result" || result == "exception") {
    await redisClient.sAdd(BACKEDUP_PHOTOS_ERRORS_SET, photo);
  // } else if (result == "too many requests") {
  //   await redisClient.sAdd(PHOTOS_TO_BACKUP_SET, photo);
  } else {
    log.error(`Error updating backup result - ${photo} - Unexpected result: ${result}`);
  }
}

async function backupPhotos(job) {
  const redisClient = createClient({ url: REDIS_URL });
  try {
    await redisClient.connect();

    const start_time = new Date()
    const numBatches = Math.ceil(BACKUP_NUM_PHOTOS / BACKUP_BATCH_SIZE);
    let numPhotosBackedup = 0;
    let numPhotosErrored = 0;
    // let numPhotosRateLimit = 0;
    let numPhotosNotFound = 0;

    log.info(
      `Job ${job.id}: Going to backup ${BACKUP_NUM_PHOTOS} photos in ${numBatches} batches of ${BACKUP_BATCH_SIZE}`,
    );

    for (let i = 0; i < numBatches; i++) {
      const photos = await redisClient.sPop(
        PHOTOS_TO_BACKUP_SET,
        BACKUP_BATCH_SIZE,
      );
      if (photos.length === 0) {
        log.info(`Job ${job.id}: No more photos to backup`);
        break;
      }

      const imageToResult = Object.fromEntries(
        await Promise.all(
          photos.map(async (photo) => [photo, await backupPhoto(photo, job)]),
        ),
      );

      await Promise.all(
        photos.map(async (photo) =>
          updateResult(photo, imageToResult[photo], redisClient),
        ),
      );
      numPhotosBackedup += Object.values(imageToResult).filter((v) => v === "ok").length;
      numPhotosErrored += Object.values(imageToResult).filter((v) => v === "unexpected result" || v === "exception").length;
      // numPhotosRateLimit += Object.values(imageToResult).filter((v) => v === "too many requests").length;
      numPhotosNotFound += Object.values(imageToResult).filter((v) => v === "not found").length;
    }
    const end_time = new Date()
    log.info(
      `Job ${job.id}: Done - Backed up ${numPhotosBackedup} photos, ${numPhotosErrored} errored, ${numPhotosNotFound} not found. Took ${(end_time.getTime() - start_time.getTime()) / 1000} seconds (From ${start_time.toISOString()} to ${end_time.toISOString()})`,
    );
  } catch (error) {
    log.error(`Job ${job.id}: Error backing up photos - exception - ${error} - ${error.stack}`);
    return false;
  } finally {
    await redisClient.disconnect();
  }
}

module.exports = {
  backupPhotos,
};
