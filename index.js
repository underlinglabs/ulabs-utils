import 'dotenv/config';
import axios from 'axios';
import fs from 'fs-extra-promise';
import { S3Client, PutObjectCommand, GetObjectCommand } from '@aws-sdk/client-s3';
import { URL } from 'node:url';
import { Readable } from 'stream';
import mime from 'mime-types';
import md5File from 'md5-file';

// Download Files

async function downloadFileFromBucket(url, localPath) {
  const bp = getBucketParams(url);
  const client = new S3Client({
    credentials: {
      accessKeyId: bp.accessKeyId,
      secretAccessKey: bp.secretAccessKey,
    },
    region:bp.region,
  });
  const fileStream = fs.createWriteStream(localPath);
  const input = {
    Bucket: bp.bucket,
    Key: bp.key,
  }
  const command = new GetObjectCommand(input);
  const response = await client.send(command);
  Readable.from(response.Body).pipe(fileStream);
  await new Promise((resolve) => {
    fileStream.on('finish', resolve);
  });
}

async function downloadFileFromWeb(url, localPath) {
  const { data } = await axios.get(url, {
    responseType: 'arraybuffer',
  });
  await fs.writeFileAsync(localPath, data);
}


async function downloadFile(url, localPath) {
  if (url.startsWith('http')) {
    return downloadFileFromWeb(url, localPath);
  } else if (url.startsWith('s3://')) {
    return downloadFileFromBucket(url, localPath);
  } else {
    throw new Error('Unknown file thing');
  }
}

function getBucketParams(url) {
  // Assuming bucket url in this format
  // https://accesskey:secretkey@endpoint/bucket/key
  const u = new URL(url);
  return {
    accessKeyId: u.username,
    secretAccessKey: u.password,
    endpoint: u.hostname,
    bucket: u.pathname.split('/')[1],
    region: u.hostname.split('.')[2],
    key: u.pathname.split('/').slice(2).join('/'),
  };
}

async function uploadFile(localPath, url, contentType) {
  const md5 = await md5File(localPath);
  const md5Base64 = Buffer.from(md5, 'hex').toString('base64');
  const bp = getBucketParams(url);
  const data = await fs.readFileAsync(localPath);
  if (!contentType) {
    contentType = mime.lookup(localPath);
  }

  const client = new S3Client({
    credentials: {
      accessKeyId: bp.accessKeyId,
      secretAccessKey: bp.secretAccessKey,
    },
    region:bp.region,
  });

  const command = new PutObjectCommand({
    Bucket: bp.bucket,
    Key: bp.key,
    Body: data,
    ContentType: contentType,
    ContentMD5: md5Base64,
  });
  const resp = await client.send(command);
}

async function uploadFolder(localFolder, url) {
  const files = await fs.readdirAsync(localFolder);
  for (let file of files) {
    await uploadFile(`${localFolder}/${file}`, `${url}/${file}`);
  }
}

const TEMP_ROOT = process.env.AWS_LAMBDA_FUNCTION_NAME ? '/tmp' : './output';
console.log('USE TEMP ROOT', TEMP_ROOT);

async function getTempFolder() {
  return TEMP_ROOT;
}

async function getTempPath(relativePath) {
  await fs.ensureDirAsync(TEMP_ROOT);
  return `${TEMP_ROOT}/${relativePath}`;
}

export {
  downloadFile,
  uploadFile,
  uploadFolder,
  getTempFolder,
  getTempPath,
};

try {
  fs.emptydirSync(TEMP_ROOT);
} catch (err) {
  console.log('Unable to empty', TEMP_ROOT);
}
