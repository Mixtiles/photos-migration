const { MongoClient } = require('mongodb');
const cloudinary = require('cloudinary');
const { uploadStreamToS3 } = require('./common');
const axios = require('axios');
const { createClient } = require('redis');
const { log } = require('./log');

const {
    MONGO_URL,
    MONGO_DB_NAME,
    BATCH_SIZE,
    DRY_RUN,
    MAX_PHOTOS_PER_DAY,
    UPLOADS_TRANSFORMED_BUCKET,
    CLOUDINARY_API_KEY,
    CLOUDINARY_API_SECRET,
    CLOUDINARY_CLOUD_NAME,
    redisOptions
} = require('./env_vars');


const IMAGE_FORMATS = [
    'gif', 'png', 'jpg', 'bmp', 'ico', 'pdf', 'tiff', 'eps', 'jpc', 'jp2', 'psd', 'webp', 
    'zip', 'svg', 'webm', 'wdp', 'hpx', 'djvu', 'ai', 'flif', 'bpg', 'miff', 'tga', 'heic'
]


// Cloudinary upload that are:
// - in "mixtiles-art" env, or 
// - inside a folder in "mixtiles" env, or
// - a GDPR file
//  are not part of this migration
const ALLOWED_CLOUDINARY_UPLOADS = /\/mixtiles-art\/|\/3D\/|\/around\/|\/art\/|\/assets\/|\/centerpieces\/|\/Collages\/|\/contentTiles\/|\/creators\/|\/Daydream\/|\/direct-mail\/|\/filterImages\/|\/fonts\/|\/integration-tests\/|\/lifecycle\/|\/marketingData\/|\/Mixtiles Create\/|\/photoWalls\/|\/reactNext\/|\/samples\/|\/static\/|\/stock\/|\/templates\/|\/test\/|\/tiktok_brief\/|\/undefined\/|\/wallDecorationPrototype\/|\/whiteBackgroundFrames\/|\/wowDashboard\/|\/300x300_bgqc68.png|\/200x200_vfvp8y.png|\/100x100_ypgpb7.png/


cloudinary.config({
  cloud_name: CLOUDINARY_CLOUD_NAME,
  api_key: CLOUDINARY_API_KEY,
  api_secret: CLOUDINARY_API_SECRET,
})


async function migratePhotosFromDate (job) {
    const mongoClient = new MongoClient(MONGO_URL);
    const redisClient = createClient(redisOptions)
    let wasLocked = false;
    try {
        const dateStr = job.data.date;

        await mongoClient.connect();
        const db = mongoClient.db(MONGO_DB_NAME);
        log.info(`Date ${dateStr}: Connected successfully to Mongo server`);

        await redisClient.connect();
        log.info(`Date ${dateStr}: Connected successfully to Redis server`);
        wasLocked = await redisClient.setNX(`lock:${dateStr}`, job.id);
        if (!wasLocked) {
            lockingJobId = await redisClient.get(`lock:${dateStr}`);
            const error = `Date ${dateStr}: Already locked by job id ${lockingJobId}!`
            log.error(error);
            throw new Error(error);
        }

        const photos = await getPhotos(dateStr, db);
        log.info(`Date ${dateStr}: Found ${photos.length} photos to migrate`);

        let numPhotosMigrated = 0;
        for (let i = 0; i < photos.length; i += BATCH_SIZE) {
            const batch = photos.slice(i, i + BATCH_SIZE);
            const results = await Promise.all(batch.map(photo => migratePhoto(dateStr, photo, db)));
            results.forEach(result => { numPhotosMigrated += result });
            job.progress((Math.min(i + BATCH_SIZE, photos.length)) / photos.length * 100);
        }
        
        return { status: 'Success', numPhotosQueried: photos.length, numPhotosMigrated};
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
            '$or': [
                { bigThumb: /res.cloudinary.com.*\/upload\// },
                { fullsize: /res.cloudinary.com.*\/upload\// },
                { jigVersion: /res.cloudinary.com.*\/(upload|private)\// },
                { mediumThumb: /res.cloudinary.com.*\/upload\// },
                { photoUrl: /res.cloudinary.com.*\/upload\// },
                { previewThumbnail: /res.cloudinary.com.*\/upload\// },
                { smallThumb: /res.cloudinary.com.*\/upload\// },
            ],
            fullsize: {
                '$ne': 'https://res.cloudinary.com/mixtiles/image/upload/v1527783983/300x300_bgqc68.png'
            }
        }
    ).project(
        {
            _id: 1,
            _created_at: 1,
            url: 1,
            bigThumb: 1,
            fullsize: 1,
            jigVersion: 1,
            mediumThumb: 1,
            photoUrl: 1,
            previewThumbnail: 1,
            smallThumb: 1,
        }
    ).limit(MAX_PHOTOS_PER_DAY).toArray()
}

async function migratePhoto(dateStr, photo, db) {
    try {

        log.info(`Date ${dateStr}: Photo ${photo._id}: Start migrating...`);

        if (!photo.jigVersion) {
            log.info(`Date ${dateStr}: Photo ${photo._id}: No jigVersion, skipping...`);
            return 0;
        }

        const before = {
            url: photo.url,
            jigVersion: photo.jigVersion,
            bigThumb: photo.bigThumb,
            fullsize: photo.fullsize,
            mediumThumb: photo.mediumThumb,
            photoUrl: photo.photoUrl,
            previewThumbnail: photo.previewThumbnail,
            smallThumb: photo.smallThumb,
        }
        const after = {}
        const migratedFiles = []

        if (
            !photo.jigVersion.match(/res.cloudinary.com.*\/(upload|private)\//) ||
            !photo.photoUrl.match(/res.cloudinary.com.*\/upload\//) ||
            !photo.bigThumb.match(/res.cloudinary.com.*\/upload\//) ||
            !photo.fullsize.match(/res.cloudinary.com.*\/upload\//) ||
            !photo.mediumThumb.match(/res.cloudinary.com.*\/upload\//) ||
            !photo.previewThumbnail.match(/res.cloudinary.com.*\/upload\//) ||
            !photo.smallThumb.match(/res.cloudinary.com.*\/upload\//)
        ) {
            if (
                photo.jigVersion.match(/res.cloudinary.com.*\/fetch\//) &&
                photo.bigThumb.match(/res.cloudinary.com.*\/fetch\//) &&
                photo.fullsize.match(/res.cloudinary.com.*\/fetch\//) &&
                photo.mediumThumb.match(/res.cloudinary.com.*\/fetch\//) &&
                photo.previewThumbnail.match(/res.cloudinary.com.*\/fetch\//) &&
                photo.smallThumb.match(/res.cloudinary.com.*\/fetch\//) &&
                photo.photoUrl.match(/res.cloudinary.com.*\/upload\//) &&
                photo.photoUrl.match(ALLOWED_CLOUDINARY_UPLOADS)
            ) {
                log.info(`Date ${dateStr}: Photo ${photo._id}: only photoUrl is Cloudinary upload, of an allowed type (meaning already migrated), skipping...`);
                return 0;    
            } else {
                const error = `Date ${dateStr}: Photo ${photo._id}: Unlike expected, not all 7 photos are Cloudinary upload!`
                log.error(error);
                throw new Error(error);
            }
        }
        if (
            (photo.jigVersion.match(/\/(upload|private)\//g) || []).length != 1 ||
            (photo.photoUrl.match(/\/upload\//g) || []).length != 1 ||
            (photo.bigThumb.match(/\/upload\//g) || []).length != 1 ||
            (photo.fullsize.match(/\/upload\//g) || []).length != 1 ||
            (photo.mediumThumb.match(/\/upload\//g) || []).length != 1 ||
            (photo.previewThumbnail.match(/\/upload\//g) || []).length != 1 ||
            (photo.smallThumb.match(/\/upload\//g) || []).length != 1
        ) {
            const error = `Date ${dateStr}: Photo ${photo._id}: Unlike expected, not all 7 photos have exactly 1 aprearance of /upload/ in their URL!`
            log.error(error);
            throw new Error(error);
        }
        if (
            (photo.jigVersion.match(/\/fetch\//g) || []).length != 0 ||
            (photo.photoUrl.match(/\/fetch\//g) || []).length != 0 ||
            (photo.bigThumb.match(/\/fetch\//g) || []).length != 0 ||
            (photo.fullsize.match(/\/fetch\//g) || []).length != 0 ||
            (photo.mediumThumb.match(/\/fetch\//g) || []).length != 0 ||
            (photo.previewThumbnail.match(/\/fetch\//g) || []).length != 0 ||
            (photo.smallThumb.match(/\/fetch\//g) || []).length != 0
        ) {
            const error = `Date ${dateStr}: Photo ${photo._id}: Unlike expected, not all 7 photos have exactly 0 aprearance of /fetch/ in their URL!`
            log.error(error);
            throw new Error(error);
        }

        const jigVersionFileName = photo.jigVersion.split('/').pop();
        const photoUrlFileName = photo.photoUrl.split('/').pop();
        const bigThumbFileName = photo.bigThumb.split('/').pop();
        const fullsizeFileName = photo.fullsize.split('/').pop();
        const mediumThumbFileName = photo.mediumThumb.split('/').pop();
        const previewThumbnailFileName = photo.previewThumbnail.split('/').pop();
        const smallThumbFileName = photo.smallThumb.split('/').pop();

        const jigVersionBaseFileName = jigVersionFileName.split('.')[0]
        const photoUrlBaseFileName = photoUrlFileName.split('.')[0]

        if (
            !bigThumbFileName == photoUrlFileName ||
            !fullsizeFileName == photoUrlFileName ||
            !mediumThumbFileName == photoUrlFileName ||
            !previewThumbnailFileName == photoUrlFileName ||
            !smallThumbFileName == photoUrlFileName
        ) {
            const error = `Date ${dateStr}: Photo ${photo._id}: Unlike expected, not all 6 photos are the same Cloudinary upload!`
            log.error(error);
            throw new Error(error);
        }
        if (
            !photo.photoUrl.match(/res.cloudinary.com.*\/image\/upload\//) ||
            !photo.bigThumb.match(/res.cloudinary.com.*\/image\/upload\//) ||
            !photo.fullsize.match(/res.cloudinary.com.*\/image\/upload\//) ||
            !photo.mediumThumb.match(/res.cloudinary.com.*\/image\/upload\//) ||
            !photo.previewThumbnail.match(/res.cloudinary.com.*\/image\/upload\//) ||
            !photo.smallThumb.match(/res.cloudinary.com.*\/image\/upload\//)
        ) {
            const error = `Date ${dateStr}: Photo ${photo._id}: Unlike expected, not all 6 photos include /image/upload/ in their URL!`
            log.error(error);
            throw new Error(error);
        }

        if (jigVersionFileName.endsWith('.pdf')) {
            log.info(`Date ${dateStr}: Photo ${photo._id}: jigVersion is a PDF.`);
            const { downloadUrl, publicId, format } = await getCloudinaryComponents(dateStr, photo, photo.jigVersion);
            after.jigVersion = await migrateToS3(dateStr, photo, downloadUrl, publicId, format);
            migratedFiles.push({from: downloadUrl, to: after.jigVersion, fromFormat: format, fromPublicId: publicId});
        } else {
            log.info(`Date ${dateStr}: Photo ${photo._id}: jigVersion is an image.`);
            if (!jigVersionFileName == photoUrlFileName) {
                const error = `Date ${dateStr}: Photo ${photo._id}: Unlike expected, non-PDF jigVersion is not same Cloudinary upload!`
                log.error(error);
                throw new Error(error);
            }
            if (!photo.jigVersion.match(/res.cloudinary.com.*\/image\/upload\//)) {
                const error = `Date ${dateStr}: Photo ${photo._id}: Unlike expected, non-PDF jigVersion doesn't include /image/upload/ in its URL!`
                log.error(error);
                throw new Error(error);
            }
        }

        let s3Url
        if (photoUrlBaseFileName != jigVersionBaseFileName || photo.url.match(/cloudinary/)) {
            const { downloadUrl, publicId, format } = await getCloudinaryComponents(dateStr, photo, photo.fullsize)
            s3Url = await migrateToS3(dateStr, photo, downloadUrl, publicId, format)
            migratedFiles.push({from: downloadUrl, to: s3Url, fromFormat: format, fromPublicId: publicId});
        } else {
            // s3Url here means non-Cloudinary. Almost always it's S3, but it still could be in other domains (filestck.com, etc.)
            s3Url = photo.url
        }

        if (!after.jigVersion) {
            after.jigVersion = transformToFetchUrl(photo.jigVersion, s3Url);
        }
        after.photoUrl = photo.url
        after.bigThumb = transformToFetchUrl(photo.bigThumb, s3Url);
        after.fullsize = transformToFetchUrl(photo.fullsize, s3Url);
        after.mediumThumb = transformToFetchUrl(photo.mediumThumb, s3Url);
        after.previewThumbnail = transformToFetchUrl(photo.previewThumbnail, s3Url);
        after.smallThumb = transformToFetchUrl(photo.smallThumb, s3Url);

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



async function migrateToS3(dateStr, photo, downloadUrl, publicId, format) {
    log.info(`Date ${dateStr}: Photo ${photo._id}: Migrating ${downloadUrl} to S3 file ${publicId}_migrated.${format} (Bucket: ${UPLOADS_TRANSFORMED_BUCKET})...`)
    return (
        await uploadStreamToS3({
            stream: (await axios.get(downloadUrl, { responseType: 'stream' })).data,
            filename: `${publicId}_migrated.${format}`,
            bucketName: UPLOADS_TRANSFORMED_BUCKET,
        })
    ).url
}

function transformToFetchUrl(cloudinaryUrl, s3Url) {
    const lastSlashIndex = cloudinaryUrl.lastIndexOf("/");
    return cloudinaryUrl.
        substring(0, lastSlashIndex).
        replace("/image/upload/", "/image/fetch/") +
        '/f_jpg/' + s3Url 
}

module.exports = {
  migratePhotosFromDate
}