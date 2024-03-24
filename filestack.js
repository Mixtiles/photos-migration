const csv = require('csv-parser');
const { promisify } = require('util');
const fs = require('fs');
const pipeline = promisify(require('stream').pipeline);
const path = require('path');
const { log } = require('./log');

async function loadCsv(filePath) {
    const handleIdToPath = {};
    await pipeline(
        fs.createReadStream(filePath),
        csv(),
        async (data) => {
            for await (const row of data) {
                handleIdToPath[row.handle] = row.path;
            }
        }
    );
    return handleIdToPath;
}

let filestackHandleIdToPath;

async function getFilestackHandleIdToPath() {
    if (!filestackHandleIdToPath) {
        log.info('Loading filestack handle to path mapping...');
        /***
         * The file was created from BigQuery with the following query:
         * SELECT handle, SUBSTR(path, CHAR_LENGTH('filestack-mixtiles-uploads/')) as path
         * FROM mixtiles-148323.gofman_test.filestack_files 
         * WHERE TIMESTAMP(created_at) >= TIMESTAMP('2024-01-20')
         * AND path IS NOT NULL
         */
        filestackHandleIdToPath = await loadCsv(path.join(__dirname, 'filestack_handle_to_path.csv'));
        log.info('Filestack handle to path mapping loaded.');
    }
    return filestackHandleIdToPath;
}

module.exports = { getFilestackHandleIdToPath };