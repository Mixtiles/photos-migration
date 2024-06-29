const { createClient } = require("redis");
const { log } = require("./log");
const cloudinary = require("cloudinary");

const {
  REDIS_URL,
  DELETE_BATCH_SIZE,
  DELETE_NUM_PHOTOS,
  CLOUDINARY_API_KEY,
  CLOUDINARY_API_SECRET,
  CLOUDINARY_CLOUD_NAME,
} = require("./env_vars");

const DELETED_PHOTOS_SET = "deletedPhotos";
const DELETED_PHOTOS_ERRORS_SET = "deletedPhotosErrors";
const PHOTOS_TO_DELETE_SET = "photosToDelete";

cloudinary.config({
  cloud_name: CLOUDINARY_CLOUD_NAME,
  api_key: CLOUDINARY_API_KEY,
  api_secret: CLOUDINARY_API_SECRET,
});

async function deletePhoto(photo, job) {
  try {
    const res = await cloudinary.v2.uploader.destroy(photo);
    if (res.result === "ok") {
      log.info(
        `Job ${job.id}: Photo deleted successfully - ${photo}: ${res.result}`,
      );
      return "ok";
    } else {
      log.error(
        `Job ${job.id}: Error deleting photo - unexpected result - ${photo}: ${res.result}`,
      );
      return "unexpected result";
    }
  } catch (error) {
    if (res.result === "Too many concurrent upload_api_resource_destroy operations"){
      log.error(
        `Job ${job.id}: Error deleting photo - too many requests - ${photo}: ${JSON.stringify(error)}`,
      );
      return "too many requests";
    } else {
      log.error(
        `Job ${job.id}: Error deleting photo - exception - ${photo}: ${JSON.stringify(error)}`,
      );
      return "exception";
    }
  }
}

async function updateResult(photo, result, redisClient) {
  if (result == "ok") {
    await redisClient.sAdd(DELETED_PHOTOS_SET, photo);
  } else if (result == "unexpected result" || result == "exception") {
    await redisClient.sAdd(DELETED_PHOTOS_ERRORS_SET, photo);
  } else if (result == "too many requests") {
    await redisClient.sAdd(PHOTOS_TO_DELETE_SET, photo);
  } else {
    log.error(`Error updaing result - ${photo} - Unexpected result: ${result}`);
  }
}

async function deletePhotos(job) {
  const redisClient = createClient({ url: REDIS_URL });
  try {
    a = b
    await redisClient.connect();

    const start_time = new Date()
    const numBatches = Math.ceil(DELETE_NUM_PHOTOS / DELETE_BATCH_SIZE);
    let numPhotosDeleted = 0;
    let numPhotosErrored = 0;

    log.info(
      `Job ${job.id}: Going to delete ${DELETE_NUM_PHOTOS} photos in ${numBatches} batches of ${DELETE_BATCH_SIZE}`,
    );

    for (let i = 0; i < numBatches; i++) {
      const photos = await redisClient.sPop(
        PHOTOS_TO_DELETE_SET,
        DELETE_BATCH_SIZE,
      );
      if (photos.length === 0) {
        log.info(`Job ${job.id}: No more photos to delete`);
        break;
      }

      const imageToResult = Object.fromEntries(
        await Promise.all(
          photos.map(async (photo) => [photo, await deletePhoto(photo, job)]),
        ),
      );

      await Promise.all(
        photos.map(async (photo) =>
          updateResult(photo, imageToResult[photo], redisClient),
        ),
      );
      numPhotosDeleted += Object.values(imageToResult).filter(Boolean).length;
      numPhotosErrored += Object.values(imageToResult).filter((v) => !v).length;
    }
    const end_time = new Date()
    log.info(
      `Job ${job.id}: Done - Deleted ${numPhotosDeleted} photos, ${numPhotosErrored} errored. Took ${(end_time.getTime() - start_time.getTime()) / 1000} seconds (From ${start_time.toISOString()} to ${end_time.toISOString()})`, 
    );
  } catch (error) {
    log.error(`Job ${job.id}: Error deleting photos - exception - ${error} - ${error.stack}`);
    return false;
  } finally {
    await redisClient.disconnect();
  }
}

module.exports = {
  deletePhotos,
};
