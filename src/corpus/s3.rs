use s3::{
    types::{GetObjectOutput, HeadObjectOutput},
    Auth, Client,
};

pub struct S3Client {
    client: Client,
    bucket: String,
}

impl S3Client {
    pub fn new() -> anyhow::Result<Self> {
        let s3_endpoint = std::env::var("S3_ENDPOINT")?;
        let s3_region = std::env::var("S3_REGION")?;
        let s3_bucket = std::env::var("S3_BUCKET")?;

        let client = Client::builder(s3_endpoint)?
            .region(s3_region)
            .auth(Auth::from_env()?)
            .build()?;

        Ok(Self {
            client,
            bucket: s3_bucket,
        })
    }

    pub async fn head(self, object: String) -> s3::Result<HeadObjectOutput> {
        let req = self
            .client
            .objects()
            .head(self.bucket, object)
            .send()
            .await?;
        Ok(req)
    }

    pub async fn get(&self, object: String) -> s3::Result<GetObjectOutput> {
        let req = self
            .client
            .objects()
            .get(self.bucket.clone(), object)
            .send()
            .await?;
        Ok(req)
    }
}
