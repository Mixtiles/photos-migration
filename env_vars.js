const PORT = parseInt(process.env.PORT)
const REDIS_URL = process.env.REDIS_URL
const WEB_CONCURRENCY = parseInt(process.env.WEB_CONCURRENCY);
const MONGO_URL = process.env.MONGO_URL
const MONGO_DB_NAME = process.env.MONGO_DB_NAME
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE)
const DRY_RUN = process.env.DRY_RUN;
const MAX_PHOTOS_PER_DAY = parseInt(process.env.MAX_PHOTOS_PER_DAY);
const UPLOADS_TRANSFORMED_BUCKET = process.env.UPLOADS_TRANSFORMED_BUCKET
const FILESTACK_BUCKET = process.env.FILESTACK_BUCKET
const CLOUDINARY_API_KEY = process.env.CLOUDINARY_API_KEY
const CLOUDINARY_API_SECRET = process.env.CLOUDINARY_API_SECRET
const CLOUDINARY_CLOUD_NAME = process.env.CLOUDINARY_CLOUD_NAME
const AWS_ACCESS_KEY_ID = process.env.AWS_ACCESS_KEY_ID
const AWS_SECRET_ACCESS_KEY = process.env.AWS_SECRET_ACCESS_KEY
const LOCAL = process.env.LOCAL ?? false
const MAX_ACTIVE_JOBS = parseInt(process.env.MAX_ACTIVE_JOBS)

if (!PORT || !REDIS_URL || !WEB_CONCURRENCY || !MONGO_URL || !MONGO_DB_NAME || 
    !BATCH_SIZE || !DRY_RUN || !MAX_PHOTOS_PER_DAY || !UPLOADS_TRANSFORMED_BUCKET || 
    !FILESTACK_BUCKET || !CLOUDINARY_API_KEY || !CLOUDINARY_API_SECRET || !CLOUDINARY_CLOUD_NAME || 
    !AWS_ACCESS_KEY_ID || !AWS_SECRET_ACCESS_KEY || !MAX_ACTIVE_JOBS
) {
    throw new Error('One or more environment variables are missing!')
}

const redisUri = new URL(REDIS_URL)
const redisOptions = {
    port: Number(redisUri.port),
    host: redisUri.hostname,
    username: redisUri.username,
    password: redisUri.password,
    tls: redisUri.protocol === 'rediss:' && {
    rejectUnauthorized: false,
    }
}

module.exports = {
    PORT,
    REDIS_URL,
    WEB_CONCURRENCY,
    MONGO_URL,
    MONGO_DB_NAME,
    BATCH_SIZE,
    DRY_RUN,
    MAX_PHOTOS_PER_DAY,
    UPLOADS_TRANSFORMED_BUCKET,
    FILESTACK_BUCKET,
    CLOUDINARY_API_KEY,
    CLOUDINARY_API_SECRET,
    CLOUDINARY_CLOUD_NAME,
    LOCAL,
    MAX_ACTIVE_JOBS,
    redisOptions
}