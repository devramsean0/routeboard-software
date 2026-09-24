use std::{fs::{self, File}, io::{BufReader, Read}};
use log::{debug, info};
use flate2::read::GzDecoder;
use rdkafka::message::ToBytes;
use futures::TryStreamExt;

mod s3;

pub async fn update_corpus() -> anyhow::Result<TiplocData> {
    let cache_dir = std::env::var("CACHE_DIR").unwrap_or_else(|_| "cache".to_string());

    if decide_on_download().await? {
        let client = s3::S3Client::new()?;
        let s3_file_body = client.get("CORPUSExtract.json.gz".to_string()).await?;

        let mut body_stream = s3_file_body.body;
        let mut bytes = Vec::new();
        while let Some(chunk) = body_stream.try_next().await? {
            bytes.extend_from_slice(&chunk);
        }
        fs::create_dir_all(cache_dir.clone())?;
        fs::write(format!("{cache_dir}/CORPUSExtract.json.gz"), bytes.clone())?;
    }

    let file = fs::read(format!("{cache_dir}/CORPUSExtract.json.gz"))?;
    let mut decompressor = GzDecoder::new(file.to_bytes());
    let mut json_string = String::new();
    decompressor.read_to_string(&mut json_string).unwrap();
    //fs::write("cache/CORPUSExtract.json", json_string.clone())?;

    let json = serde_json::from_str::<TiplocData>(json_string.as_str())?;
    info!("Corpus Length: {}", json.tiplocdata.len());

    Ok(json)
}

// Format: {"NLC": 999078, "STANOX": " ", "TIPLOC": " ", "3ALPHA": " ", "UIC": " ", "NLCDESC": "AXIS/ACRES", "NLCDESC16": " "}

#[derive(serde::Deserialize)]
pub struct TiplocData {
    #[serde(rename = "TIPLOCDATA")]
    pub tiplocdata: Vec<LocationEntry>,
}

#[derive(serde::Deserialize)]
pub struct LocationEntry {
    #[serde(rename = "STANOX")]
    pub stanox: Option<String>,
    #[serde(rename = "3ALPHA")]
    pub crs: Option<String>,
}

async fn decide_on_download() -> anyhow::Result<bool> {
    let cache_dir = std::env::var("CACHE_DIR").unwrap_or_else(|_| "cache".to_string());
    if !fs::exists(format!("{cache_dir}/CORPUSExtract.json.gz"))? {
        info!("Downloading because cache not found");
        return Ok(true);
    } else {
        if std::env::var("ENABLE_CORPUS_UPDATE").unwrap_or_default() == "true" {
            info!("Downloading because environment variable set");
            let client = s3::S3Client::new()?;
            let s3_file_head = client.head("CORPUSExtract.json.gz".to_string()).await?;
            let s3_md5 = s3_file_head.etag.unwrap().replace("\"", "");
            let md5 = md5_of_file("cache/CORPUSExtract.json.gz").unwrap();

            debug!("S3: {}, local: {md5}", s3_md5);
            if s3_md5 == md5 {
                info!("Skipping download because cache matches");
                return Ok(false);
            }
            info!("Downloading because cache doesn't match");
        } else {
            return Ok(false);
        }
        Ok(true)
    }
}

pub fn md5_of_file(path: &str) -> Result<String, Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut context = md5::Context::new();
    let mut buffer = [0u8; 8192];
    loop {
        let bytes_read = reader.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }
        context.consume(&buffer[..bytes_read]);
    }
    let digest = context.finalize();
    Ok(format!("{:x}", digest))
}