// This is for latest photos that were found with `url` field that
// is a Cloudinary upload.
// See: https://www.notion.so/mixtiles/Testing-at-the-End-of-the-Migration-86c492fdd3654a01baf59ba64f55ad5d?pvs=4#a5dff727bbef47e5bec01490f9d5d0dc

const { MongoClient } = require('mongodb');
const cloudinary = require('cloudinary');
const { uploadStreamToS3 } = require('./common');
const axios = require('axios');
const { createClient } = require('redis');
const { log } = require('./log');

const {
    REDIS_URL,
    MONGO_URL,
    MONGO_DB_NAME,
    BATCH_SIZE,
    DRY_RUN,
    MAX_PHOTOS_PER_DAY,
    UPLOADS_TRANSFORMED_BUCKET,
    CLOUDINARY_API_KEY,
    CLOUDINARY_API_SECRET,
    CLOUDINARY_CLOUD_NAME,
} = require('./env_vars');


const IMAGE_FORMATS = [
    'gif', 'png', 'jpg', 'bmp', 'ico', 'pdf', 'tiff', 'eps', 'jpc', 'jp2', 'psd', 'webp', 
    'zip', 'svg', 'webm', 'wdp', 'hpx', 'djvu', 'ai', 'flif', 'bpg', 'miff', 'tga', 'heic'
]


cloudinary.config({
  cloud_name: CLOUDINARY_CLOUD_NAME,
  api_key: CLOUDINARY_API_KEY,
  api_secret: CLOUDINARY_API_SECRET,
})


async function migratePhotosFromDate (job) {
    const mongoClient = new MongoClient(MONGO_URL);
    const redisClient = createClient({ url: REDIS_URL})
    let wasLocked = false;
    try {
        const dateStr = job.data.date;

        log.info(`Date ${dateStr}: Connecting to Mongo server...`);
        await mongoClient.connect();
        const db = mongoClient.db(MONGO_DB_NAME);
        log.info(`Date ${dateStr}: Connected successfully to Mongo server`);

        log.info(`Date ${dateStr}: Connecting to Redis server...`);
        await redisClient.connect();
        log.info(`Date ${dateStr}: Connected successfully to Redis server`);
        wasLocked = await redisClient.setNX(`lock:${dateStr}`, job.id);
        if (!wasLocked) {
            lockingJobId = await redisClient.get(`lock:${dateStr}`);
            if (lockingJobId != job.id) {
                const error = `Date ${dateStr}: Already locked by job id ${lockingJobId}!`
                log.error(error);
                throw new Error(error);
            }
        }

        const photos = await getPhotos(dateStr, db);
        log.info(`Date ${dateStr}: Found ${photos.length} photos to migrate`);

        let numPhotosMigrated = 0;
        for (let i = 0; i < photos.length; i += BATCH_SIZE) {
            const batch = photos.slice(i, i + BATCH_SIZE);
            const results = await Promise.all(batch.map(photo => migratePhoto(dateStr, photo, db, job)));
            results.forEach(result => { numPhotosMigrated += result });
            job.progress((Math.min(i + BATCH_SIZE, photos.length)) / photos.length * 100);

            const shouldStop = await redisClient.get(`stop:${dateStr}`)
            if (shouldStop) {
                log.info(`Date ${dateStr}: Stopping job...`);
                return { status: 'Stopped', numPhotosQueried: photos.length, numPhotosMigrated};
            }
        }
        
        return { status: 'Success', numPhotosQueried: photos.length, numPhotosMigrated, date: dateStr};
    } catch (err) {
        log.error(err);
        throw err;
    } finally {
        // Ensures closing on finish / error
        await mongoClient.close();
        if (wasLocked) {
            const numLocksDeleted = await redisClient.del(`lock:${job.data.date}`);
            if (numLocksDeleted != 1) {
                log.error(`Date ${dateStr}: Failed to delete lock "lock:${job.data.date}"! numLocksDeleted=${numLocksDeleted}!`);
            }
        }
        await redisClient.disconnect();
    }
}

async function getPhotos(dateStr, db) {
    const date = new Date(dateStr);
    const dayAfter = new Date(date.getTime() + (24 * 60 * 60 * 1000));

    return db.collection('Photo').find(
        {
            _created_at: {
                $gte: date,
                $lt: dayAfter
            },
            jigVersion:{ $exists: false },
            fullsize:{ $exists: false },
            previewThumbnail:{ $exists: false },
            smallThumb:{ $exists: false },
            mediumThumb:{ $exists: false },
            bigThumb:{ $exists: false },
            photoUrl:{ $exists: false },
            $and: [
                {
                    url: {
                        $regex: /https?:\/\/res.cloudinary.com\/mixtiles\/image\/upload\/.*/,
                    },
                },
                {
                    url: {
                        $not: {
                            $regex: /.*res.cloudinary.com\/mixtiles\/image\/upload.*\/assets\/.*/,
                        }
                    }
                },
                {
                    url: {
                        $not: {
                            $regex: /.*res.cloudinary.com\/mixtiles\/image\/upload.*%252Fassets%252F.*/,
                        }
                    }
                },
                {
                    url: {
                        $not: {
                            $regex: /.*res.cloudinary.com\/mixtiles\/image\/upload.*%2Fassets%2F.*/,
                        }
                    }
                },
                {
                    url: {
                        $not: {
                            $regex: /.*res.cloudinary.com\/mixtiles\/image\/upload.*\/centerpieces\/.*/,
                        }
                    }
                },
                {
                    url: {
                        $not: {
                            $regex: /.*res.cloudinary.com\/mixtiles\/image\/upload.*%2Fcenterpieces%2F.*/,
                        }
                    }
                },
                {
                    url: {
                        $not: {
                            $regex: /.*res.cloudinary.com\/mixtiles\/image\/upload.*\/Daydream\/.*/,
                        }
                    }
                },
                {
                    url: {
                        $not: {
                            $regex: /.*res.cloudinary.com\/mixtiles\/image\/upload.*\/creators\/.*/,
                        }
                    }
                },
                {
                    url: {
                        $not: {
                            $regex: /.*res.cloudinary.com\/mixtiles\/image\/upload.*\/contentTiles\/.*/,
                        }
                    }
                },
                {
                    url: {
                        $not: {
                            $regex: /.*res.cloudinary.com\/mixtiles\/image\/upload.*\/art\/.*/,
                        }
                    }
                },
                {
                    url: {
                        $not: {
                            $regex: /.*res.cloudinary.com\/mixtiles\/image\/upload.*\/Mixtiles%20Create\/.*/,
                        }
                    }
                }
            ]
        }
    ).project(
        {
            _id: 1,
            _created_at: 1,
            url: 1,
        }
    ).limit(MAX_PHOTOS_PER_DAY).toArray()
}

async function migratePhoto(dateStr, photo, db, job) {
    try {
        log.info(`Date ${dateStr}: Photo ${photo._id}: Start migrating...`);

        const before = {
            url: photo.url,
        }
        const after = {}
        const migratedFiles = []

        log.info(`Date ${dateStr}: Photo ${photo._id}: Url is in cloudinary. Migrating from Cloudinary.`);
        const { downloadUrl, publicId, format } = await getCloudinaryComponents(dateStr, photo, photo.url)
        s3Url = await migrateToS3(dateStr, photo, downloadUrl, publicId + '_url', format, UPLOADS_TRANSFORMED_BUCKET)
        migratedFiles.push({from: downloadUrl, to: s3Url, fromFormat: format, fromPublicId: publicId});
        after.url = s3Url
        
        if (DRY_RUN != 'false') {
            log.info(`Date ${dateStr}: Photo ${photo._id}: Dry run, not updating...`);
            log.info(`Date ${dateStr}: Photo ${photo._id}: Before: ${JSON.stringify(before)}`);
            log.info(`Date ${dateStr}: Photo ${photo._id}: After: ${JSON.stringify(after)}`);
            log.info(`Date ${dateStr}: Photo ${photo._id}: Migrated files: ${JSON.stringify(migratedFiles)}`);
        } else {
            log.info(`Date ${dateStr}: Photo ${photo._id}: Updating...`);
            const updateLogResult = await db.collection('PhotoMigrationLog').insertOne(
                { 
                    _created_at: new Date(),
                    jobId: job.id,
                    date: dateStr,
                    photo_id: photo._id,
                    photo_created_at: photo._created_at,
                    before,
                    after,
                    migratedFiles
                }
            );
            if (!updateLogResult.acknowledged || !updateLogResult.insertedId) {
                const error = `Date ${dateStr}: Photo ${photo._id}: Failed to insert migration log! updateLogResult=${JSON.stringify(updateLogResult)}!`
                log.error(error);
                throw new Error(error);
            }
            const updatePhotoResult = await db.collection('Photo').updateOne(
                { _id: photo._id },
                { $set: after }
            );
            if (!updatePhotoResult.acknowledged || updatePhotoResult.modifiedCount != 1) {
                const error = `Date ${dateStr}: Photo ${photo._id}: Failed to update! updatePhotoResult=${JSON.stringify(updatePhotoResult)}!`
                log.error(error);
                throw new Error(error);
            }
            log.info(`Date ${dateStr}: Photo ${photo._id}: Updated.`);
        }

        log.info(`Date ${dateStr}: Photo ${photo._id}: Done migrating.`);
        return 1;
    } catch (err) {
        log.error(`Date ${dateStr}: Photo ${photo._id}: Error - ${err}`);
        try {
            await db.collection('PhotoMigrationLog').insertOne(
                { 
                    _created_at: new Date(),
                    jobId: job.id,
                    date: dateStr,
                    photo_id: photo._id,
                    photo_created_at: photo._created_at,
                    error: err.toString(),
                }
            );
        } catch (err2) {
            log.error(`Date ${dateStr}: Photo ${photo._id}: Error updating DB with error - ${err2}`);
            return 0;
        }
        return 0;
    }
}

async function getCloudinaryComponents(dateStr, photo, cloudinaryUrl) {
    const lastSlashIndex = cloudinaryUrl.lastIndexOf("/");
    const lastDotIndex = cloudinaryUrl.lastIndexOf(".");
    const publicId = lastDotIndex > lastSlashIndex ?
        cloudinaryUrl.substring(lastSlashIndex + 1, lastDotIndex) :
        cloudinaryUrl.substring(lastSlashIndex + 1);

    let format, downloadUrl
    if (!cloudinaryUrl.endsWith('.pdf')) {
        if (lastDotIndex > lastSlashIndex) {
            format = cloudinaryUrl.substring(lastDotIndex + 1);
            cloudinaryEnv = cloudinaryUrl.match(/res.cloudinary.com\/([^\/]+)\/image\/upload/)?.[1]
            if (cloudinaryEnv != 'mixtiles' && cloudinaryEnv != 'mixtiles-dev') {
                const error = `Date ${dateStr}: Photo ${photo._id}: Cloudinary URL ${cloudinaryUrl} could not be matched to an environment that is part of this migration: "${cloudinaryEnv}"!`
                log.error(error);
                throw new Error(error);
            }
            downloadUrl = `https://res.cloudinary.com/${cloudinaryEnv}/image/upload/${publicId}.${format}`;
        } else {
            // This is the best way to get format and downloadUrl, but we have 5000 admin API calls/hours,
            // So we use it only when we don't have the format as part of the URL
            const resource = await cloudinary.v2.api.resource(publicId)
            format = resource.format
            downloadUrl = resource.url
        }

        if (!IMAGE_FORMATS.includes(format)) {
            const error = `Date ${dateStr}: Photo ${photo._id}: Cloudinary URL ${cloudinaryUrl} has invalid format: ${resource.format}!`
            log.error(error);
            throw new Error(error);
        }
    } else {
        format = 'pdf'
        downloadUrl = cloudinaryUrl
    }

    return { downloadUrl, publicId, format }
}

async function migrateToS3(dateStr, photo, downloadUrl, publicId, format, bucketName) {
    log.info(`Date ${dateStr}: Photo ${photo._id}: Migrating ${downloadUrl} to S3 file ${publicId}_migrated.${format} (Bucket: ${bucketName})...`)
    return (
        await uploadStreamToS3({
            stream: (await axios.get(downloadUrl, { responseType: 'stream' })).data,
            filename: `${publicId}_migrated.${format}`,
            bucketName: bucketName,
        })
    ).url
}

module.exports = {
  migratePhotosFromDate
}
