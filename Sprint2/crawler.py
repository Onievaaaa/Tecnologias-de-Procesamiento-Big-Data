import time
import boto3
from botocore.exceptions import ClientError


GRUPO = "imat3b10"  
GLUE_DB_NAME = f"trade_data_{GRUPO}"

CRYPTO_TABLE_HINT = "btc" 

S3_BUCKET = "btc-imat-bucket"
S3_PREFIX = "btc"  


CRAWLER_NAME = f"crawler_{CRYPTO_TABLE_HINT}_{GRUPO}"

GLUE_ROLE_ARN = "arn:aws:iam::354918392915:role/service-role/AWSGlueServiceRole-S3-Bucket"


AWS_PROFILE = "Comillas-BIGDATA-Alumnos-354918392915"
AWS_REGION = "eu-south-2"


def ensure_database(glue, db_name: str):
    """Crea la BD si no existe."""
    try:
        glue.get_database(Name=db_name)
        print(f"OK: Database ya existe: {db_name}")
    except glue.exceptions.EntityNotFoundException:
        glue.create_database(
            DatabaseInput={
                "Name": db_name,
                "Description": f"Data Catalog para {db_name}",
            }
        )
        print(f"Creada database: {db_name}")


def create_or_update_crawler(glue, crawler_name: str, role_arn: str, db_name: str, s3_bucket: str, s3_prefix: str):
    """Crea el crawler si no existe; si existe lo actualiza."""
    s3_path = f"s3://{s3_bucket}/{s3_prefix.strip('/')}/"

    crawler_def = {
        "Name": crawler_name,
        "Role": role_arn,
        "DatabaseName": db_name,
        "Targets": {
            "S3Targets": [
                {
                    "Path": s3_path,
                }
            ]
        },
        "TablePrefix": "",
        "SchemaChangePolicy": {
            "UpdateBehavior": "UPDATE_IN_DATABASE",
            "DeleteBehavior": "DEPRECATE_IN_DATABASE",
        },
        "RecrawlPolicy": {
            "RecrawlBehavior": "CRAWL_EVERYTHING"
        },
    }

    try:
        glue.get_crawler(Name=crawler_name)

        glue.update_crawler(**crawler_def)
        print(f"Actualizado crawler: {crawler_name} (target: {s3_path})")
    except glue.exceptions.EntityNotFoundException:

        glue.create_crawler(**crawler_def)
        print(f"Creado crawler: {crawler_name} (target: {s3_path})")


def start_and_wait(glue, crawler_name: str, poll_seconds: int = 10, timeout_seconds: int = 60 * 30):
    """Lanza el crawler y espera a que termine."""
    try:
        glue.start_crawler(Name=crawler_name)
        print(f"Ejecutando crawler: {crawler_name} ...")
    except glue.exceptions.CrawlerRunningException:
        print(f"El crawler ya estaba corriendo: {crawler_name}")

    start_time = time.time()
    while True:
        crawler = glue.get_crawler(Name=crawler_name)["Crawler"]
        state = crawler["State"]
        last_crawl = crawler.get("LastCrawl", {})
        status = last_crawl.get("Status") 
        print(f"Estado: {state} | LastCrawlStatus: {status}")
        if state == "READY":
            if status == "SUCCEEDED":
                print("Crawler terminado correctamente.")
            elif status == "FAILED":
                print("Crawler FAILED.")
            else:
                print("ℹCrawler READY.")
            return

        if time.time() - start_time > timeout_seconds:
            raise TimeoutError(f"Timeout esperando al crawler {crawler_name}")

        time.sleep(poll_seconds)


def main():
    session_kwargs = {"profile_name": AWS_PROFILE}
    session_kwargs["region_name"] = AWS_REGION

    session = boto3.Session(**session_kwargs)
    glue = session.client("glue")
    ensure_database(glue, GLUE_DB_NAME)
    create_or_update_crawler(
        glue=glue,
        crawler_name=CRAWLER_NAME,
        role_arn=GLUE_ROLE_ARN,
        db_name=GLUE_DB_NAME,
        s3_bucket=S3_BUCKET,
        s3_prefix=S3_PREFIX,
    )
    start_and_wait(glue, CRAWLER_NAME)


if __name__ == "__main__":
    main()
