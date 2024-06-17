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
      return true;
    } else {
      log.info(
        `Job ${job.id}: Error deleting photo - unexpected result - ${photo}: ${res.result}`,
      );
      return false;
    }
  } catch (error) {
    log.info(
      `Job ${job.id}: Error deleting photo - exception - ${photo}: ${JSON.stringify(error)}`,
    );
    return false;
  }
}

async function updateResult(photo, result, redisClient) {
  if (result == true) {
    await redisClient.sAdd(DELETED_PHOTOS_SET, photo);
  } else {
    await redisClient.sAdd(DELETED_PHOTOS_ERRORS_SET, photo);
  }
}

async function deletePhotos(job) {
  const redisClient = createClient({ url: REDIS_URL });
  try {
    await redisClient.connect();

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
    log.info(
      `Job ${job.id}: Done - Deleted ${numPhotosDeleted} photos, ${numPhotosErrored} errored`,
    );
  } catch (error) {
    log.info(`Job ${job.id}: Error deleting photos - exception - ${JSON.stringify(error)}`);
    return false;
  } finally {
    await redisClient.disconnect();
  }
}

module.exports = {
  deletePhotos,
};
